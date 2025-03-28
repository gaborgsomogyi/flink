/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.ExpressionReducer
import org.apache.flink.table.planner.plan.nodes.calcite.Rank
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRank
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalRank, StreamPhysicalWindowDeduplicate}
import org.apache.flink.table.runtime.operators.rank._

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind

import java.util

import scala.collection.JavaConversions._

/** Util for [[Rank]]s. */
object RankUtil {

  private[this] case class LimitPredicate(rankOnLeftSide: Boolean, pred: RexCall)

  sealed private[this] trait Boundary

  private[this] case class LowerBoundary(lower: Long) extends Boundary

  private[this] case class UpperBoundary(upper: Long) extends Boundary

  private[this] case class BothBoundary(lower: Long, upper: Long) extends Boundary

  private[this] case class InputRefBoundary(inputFieldIndex: Int) extends Boundary

  sealed private[this] trait BoundDefine

  private[this] object Lower extends BoundDefine // defined lower bound
  private[this] object Upper extends BoundDefine // defined upper bound
  private[this] object Both extends BoundDefine // defined lower and uppper bound

  /**
   * Extracts the TopN offset and fetch bounds from a predicate.
   *
   * @param oriPred
   *   the original predicate
   * @param rankFieldIndex
   *   the index of rank field
   * @param rexBuilder
   *   RexBuilder
   * @param tableConfig
   *   TableConfig
   * @return
   *   A Tuple2 of extracted rank range and remaining predicates.
   */
  def extractRankRange(
      oriPred: RexNode,
      rankFieldIndex: Int,
      rexBuilder: RexBuilder,
      tableConfig: TableConfig,
      classLoader: ClassLoader): (Option[RankRange], Option[RexNode]) = {
    val predicate = FlinkRexUtil.expandSearch(rexBuilder, oriPred)
    val cnfCondition = FlinkRexUtil.toCnf(rexBuilder, predicate)

    // split the condition into sort limit condition and other condition
    val (limitPreds: Seq[LimitPredicate], otherPreds: Seq[RexNode]) = cnfCondition match {
      case c: RexCall if c.getKind == SqlKind.AND =>
        c.getOperands
          .map(identifyLimitPredicate(_, rankFieldIndex))
          .foldLeft((Seq[LimitPredicate](), Seq[RexNode]())) {
            (preds, analyzed) =>
              analyzed match {
                case Left(limitPred) => (preds._1 :+ limitPred, preds._2)
                case Right(otherPred) => (preds._1, preds._2 :+ otherPred)
              }
          }
      case rex: RexNode =>
        identifyLimitPredicate(rex, rankFieldIndex) match {
          case Left(limitPred) => (Seq(limitPred), Seq())
          case Right(otherPred) => (Seq(), Seq(otherPred))
        }
      case _ =>
        return (None, Some(predicate))
    }

    if (limitPreds.isEmpty) {
      // no valid TopN bounds.
      return (None, Some(predicate))
    }

    val sortBounds =
      limitPreds.map(computeWindowBoundFromPredicate(_, rexBuilder, tableConfig, classLoader))
    val rankRange = sortBounds match {
      case Seq(Some(LowerBoundary(x)), Some(UpperBoundary(y))) =>
        new ConstantRankRange(x, y)
      case Seq(Some(UpperBoundary(x)), Some(LowerBoundary(y))) =>
        new ConstantRankRange(y, x)
      case Seq(Some(LowerBoundary(x))) =>
        // only offset
        new ConstantRankRangeWithoutEnd(x)
      case Seq(Some(UpperBoundary(x))) =>
        // rankStart starts from one
        new ConstantRankRange(1, x)
      case Seq(Some(BothBoundary(x, y))) =>
        // nth rank
        new ConstantRankRange(x, y)
      case Seq(Some(InputRefBoundary(x))) =>
        new VariableRankRange(x)
      case _ =>
        // TopN requires at least one rank comparison predicate
        return (None, Some(predicate))
    }

    val remainCondition = otherPreds match {
      case Seq() => None
      case _ => Some(otherPreds.reduceLeft((l, r) => RelOptUtil.andJoinFilters(rexBuilder, l, r)))
    }

    (Some(rankRange), remainCondition)
  }

  /**
   * Analyzes a predicate and identifies whether it is a valid predicate for a TopN. A valid TopN
   * predicate is a comparison predicate (<, <=, =>, >) or equal predicate that accesses rank fields
   * of input rel node, the rank field reference must be on one side of the condition alone.
   *
   * Examples:
   *   - rank <= 10 => valid (Top 10)
   *   - rank + 1 <= 10 => invalid: rank is not alone in the condition
   *   - rank == 10 => valid (10th)
   *   - rank <= rank + 2 => invalid: rank on same side
   *
   * @return
   *   Either a valid time predicate (Left) or a valid non-time predicate (Right)
   */
  private def identifyLimitPredicate(
      pred: RexNode,
      rankFieldIndex: Int): Either[LimitPredicate, RexNode] = pred match {
    case c: RexCall =>
      c.getKind match {
        case SqlKind.GREATER_THAN | SqlKind.GREATER_THAN_OR_EQUAL | SqlKind.LESS_THAN |
            SqlKind.LESS_THAN_OR_EQUAL | SqlKind.EQUALS =>
          val leftTerm = c.getOperands.head
          val rightTerm = c.getOperands.last

          if (
            isRankFieldRef(leftTerm, rankFieldIndex) &&
            !accessesRankField(rightTerm, rankFieldIndex)
          ) {
            Left(LimitPredicate(rankOnLeftSide = true, c))
          } else if (
            isRankFieldRef(rightTerm, rankFieldIndex) &&
            !accessesRankField(leftTerm, rankFieldIndex)
          ) {
            Left(LimitPredicate(rankOnLeftSide = false, c))
          } else {
            Right(pred)
          }

        // not a comparison predicate.
        case _ => Right(pred)
      }
    case _ => Right(pred)
  }

  // checks if the expression is the rank field reference
  def isRankFieldRef(expr: RexNode, rankFieldIndex: Int): Boolean = expr match {
    case i: RexInputRef => i.getIndex == rankFieldIndex
    case _ => false
  }

  /**
   * Checks if an expression accesses a rank field.
   *
   * @param expr
   *   The expression to check.
   * @param rankFieldIndex
   *   The rank field index.
   * @return
   *   True, if the expression accesses a time attribute. False otherwise.
   */
  def accessesRankField(expr: RexNode, rankFieldIndex: Int): Boolean = expr match {
    case i: RexInputRef => i.getIndex == rankFieldIndex
    case c: RexCall => c.operands.exists(accessesRankField(_, rankFieldIndex))
    case _ => false
  }

  /**
   * Computes the absolute bound on the left operand of a comparison expression and whether the
   * bound is an upper or lower bound.
   *
   * @return
   *   sort boundary (lower boundary, upper boundary)
   */
  private def computeWindowBoundFromPredicate(
      limitPred: LimitPredicate,
      rexBuilder: RexBuilder,
      tableConfig: TableConfig,
      classLoader: ClassLoader): Option[Boundary] = {

    val bound: BoundDefine = limitPred.pred.getKind match {
      case SqlKind.GREATER_THAN | SqlKind.GREATER_THAN_OR_EQUAL if limitPred.rankOnLeftSide =>
        Lower
      case SqlKind.GREATER_THAN | SqlKind.GREATER_THAN_OR_EQUAL if !limitPred.rankOnLeftSide =>
        Upper
      case SqlKind.LESS_THAN | SqlKind.LESS_THAN_OR_EQUAL if limitPred.rankOnLeftSide =>
        Upper
      case SqlKind.LESS_THAN | SqlKind.LESS_THAN_OR_EQUAL if !limitPred.rankOnLeftSide =>
        Lower
      case SqlKind.EQUALS =>
        Both
    }

    val predExpression = if (limitPred.rankOnLeftSide) {
      limitPred.pred.operands.get(1)
    } else {
      limitPred.pred.operands.get(0)
    }

    (predExpression, bound) match {
      case (r: RexInputRef, Upper | Both) => Some(InputRefBoundary(r.getIndex))
      case (_: RexInputRef, Lower) => None
      case _ =>
        // reduce predicate to constants to compute bounds
        val literal = reduceComparisonPredicate(limitPred, rexBuilder, tableConfig, classLoader)
        if (literal.isEmpty) {
          None
        } else {
          // compute boundary
          val tmpBoundary: Long = literal.get
          val boundary = limitPred.pred.getKind match {
            case SqlKind.LESS_THAN if limitPred.rankOnLeftSide =>
              tmpBoundary - 1
            case SqlKind.LESS_THAN =>
              tmpBoundary + 1
            case SqlKind.GREATER_THAN if limitPred.rankOnLeftSide =>
              tmpBoundary + 1
            case SqlKind.GREATER_THAN =>
              tmpBoundary - 1
            case _ =>
              tmpBoundary
          }
          bound match {
            case Lower => Some(LowerBoundary(boundary))
            case Upper => Some(UpperBoundary(boundary))
            case Both => Some(BothBoundary(boundary, boundary))
          }
        }
    }
  }

  /**
   * Replaces the rank aggregate reference on of a predicate by a zero literal and reduces the
   * expressions on both sides to a long literal.
   *
   * @param limitPred
   *   The limit predicate which both sides are reduced.
   * @param rexBuilder
   *   A RexBuilder
   * @param tableConfig
   *   A TableConfig.
   * @return
   *   The values of the reduced literals on both sides of the comparison predicate.
   */
  private def reduceComparisonPredicate(
      limitPred: LimitPredicate,
      rexBuilder: RexBuilder,
      tableConfig: TableConfig,
      classLoader: ClassLoader): Option[Long] = {

    val expression = if (limitPred.rankOnLeftSide) {
      limitPred.pred.operands.get(1)
    } else {
      limitPred.pred.operands.get(0)
    }

    if (!RexUtil.isConstant(expression)) {
      return None
    }

    // reduce expression to literal
    val exprReducer = new ExpressionReducer(tableConfig, classLoader)
    val originList = new util.ArrayList[RexNode]()
    originList.add(expression)
    val reduceList = new util.ArrayList[RexNode]()
    exprReducer.reduce(rexBuilder, originList, reduceList)

    // extract bounds from reduced literal
    val literals = reduceList.map {
      case literal: RexLiteral => Some(literal.getValue2.asInstanceOf[Long])
      case _ => None
    }

    literals.head
  }

  def isTop1(rankRange: RankRange): Boolean = rankRange match {
    case crg: ConstantRankRange => crg.getRankStart == 1L && crg.getRankEnd == 1L
    case _ => false
  }

  def getRankNumberColumnIndex(rank: Rank): Option[Int] = {
    if (rank.outputRankNumber) {
      require(rank.getRowType.getFieldCount == rank.getInput.getRowType.getFieldCount + 1)
      Some(rank.getRowType.getFieldCount - 1)
    } else {
      require(rank.getRowType.getFieldCount == rank.getInput.getRowType.getFieldCount)
      None
    }
  }

  /**
   * Whether the given rank could be converted to [[StreamPhysicalWindowDeduplicate]].
   *
   * Returns true if the given rank is sorted by time attribute and limits 1 and its RankFunction is
   * ROW_NUMBER, else false.
   *
   * @param rank
   *   The [[FlinkLogicalRank]] node
   * @return
   *   True if the input rank could be converted to [[StreamPhysicalWindowDeduplicate]]
   */
  def canConvertToDeduplicate(rank: FlinkLogicalRank): Boolean = {
    val sortCollation = rank.orderKey
    val rankRange = rank.rankRange

    val isRowNumberType = rank.rankType == RankType.ROW_NUMBER

    val isLimit1 = rankRange match {
      case rankRange: ConstantRankRange =>
        rankRange.getRankStart == 1 && rankRange.getRankEnd == 1
      case _ => false
    }

    val inputRowType = rank.getInput.getRowType
    val isSortOnTimeAttribute = sortOnTimeAttributeOnly(sortCollation, inputRowType)

    !rank.outputRankNumber && isLimit1 && isSortOnTimeAttribute && isRowNumberType
  }

  private def sortOnTimeAttributeOnly(
      sortCollation: RelCollation,
      inputRowType: RelDataType): Boolean = {
    if (sortCollation.getFieldCollations.size() != 1) {
      return false
    }
    val firstSortField = sortCollation.getFieldCollations.get(0)
    val fieldType = inputRowType.getFieldList.get(firstSortField.getFieldIndex).getType
    FlinkTypeFactory.isProctimeIndicatorType(fieldType) ||
    FlinkTypeFactory.isRowtimeIndicatorType(fieldType)
  }

  /**
   * Checks if the given sort collation has a field collation which based on a rowtime attribute.
   */
  def sortOnRowTime(sortCollation: RelCollation, inputRowType: RelDataType): Boolean = {
    sortCollation.getFieldCollations.exists {
      firstSortField =>
        val fieldType = inputRowType.getFieldList.get(firstSortField.getFieldIndex).getType
        FlinkTypeFactory.isRowtimeIndicatorType(fieldType)
    }
  }

  /** Whether the given rank is logically a deduplication. */
  def isDeduplication(rank: Rank): Boolean = {
    !rank.outputRankNumber && rank.rankType == RankType.ROW_NUMBER && isTop1(rank.rankRange)
  }

  /**
   * Whether the given [[StreamPhysicalRank]] could be converted to
   * [[org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate]].
   */
  def canConvertToDeduplicate(rank: StreamPhysicalRank): Boolean = {
    lazy val inputInsertOnly = ChangelogPlanUtils.inputInsertOnly(rank)
    lazy val sortOnTimeAttributeOnly =
      RankUtil.sortOnTimeAttributeOnly(rank.orderKey, rank.getInput.getRowType)

    isDeduplication(rank) && inputInsertOnly && sortOnTimeAttributeOnly
  }

  /**
   * Determines if the given order key indicates that the last row should be kept for deduplication.
   */
  def keepLastDeduplicateRow(orderKey: RelCollation): Boolean = {
    // order by timeIndicator desc ==> lastRow, otherwise is firstRow
    if (orderKey.getFieldCollations.size() != 1) {
      return false
    }
    val fieldCollation = orderKey.getFieldCollations.get(0)
    fieldCollation.direction.isDescending
  }

  /**
   * Currently, append-only is not supported for mini-batch mode, however this could be supported in
   * the future. Proctime keep first row mini-batch operators are already append-only.
   *
   * <p>keepLastRow can not support append only, as always more recent record will be retracting the
   * previous one.
   */
  def outputInsertOnlyInDeduplicate(config: ReadableConfig, keepLastRow: Boolean): Boolean =
    !keepLastRow && !config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
}
