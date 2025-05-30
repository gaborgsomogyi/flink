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
package org.apache.flink.table.planner.plan.metadata

import org.apache.flink.table.planner._
import org.apache.flink.table.planner.plan.metadata.FlinkMetadata.UpsertKeys
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, Rank, WatermarkAssigner, WindowAggregate}
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalGroupAggregateBase, BatchPhysicalOverAggregate, BatchPhysicalWindowAggregateBase}
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalLookupJoin
import org.apache.flink.table.planner.plan.nodes.physical.stream._
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable
import org.apache.flink.table.planner.plan.utils.{FlinkRexUtil, RankUtil, UpsertKeyUtil}

import com.google.common.collect.ImmutableSet
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.{RelDistribution, RelNode, SingleRel}
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rex.{RexNode, RexUtil}
import org.apache.calcite.util.{Bug, ImmutableBitSet, Util}

import java.util

import scala.collection.JavaConversions._

/**
 * FlinkRelMdUpsertKeys supplies a default implementation of [[FlinkRelMetadataQuery#getUpsertKeys]]
 * for the standard logical algebra.
 */
class FlinkRelMdUpsertKeys private extends MetadataHandler[UpsertKeys] {

  override def getDef: MetadataDef[UpsertKeys] = UpsertKeys.DEF

  def getUpsertKeys(rel: TableScan, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    rel.getTable match {
      case t: IntermediateRelTable => t.upsertKeys
      case _ => mq.getUniqueKeys(rel)
    }
  }

  def getUpsertKeys(rel: Project, mq: RelMetadataQuery): JSet[ImmutableBitSet] =
    getProjectUpsertKeys(rel.getProjects, rel.getInput, mq)

  def getUpsertKeys(rel: Filter, mq: RelMetadataQuery): JSet[ImmutableBitSet] =
    FlinkRelMetadataQuery.reuseOrCreate(mq).getUpsertKeys(rel.getInput)

  def getUpsertKeys(calc: Calc, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)
    getProjectUpsertKeys(projects, calc.getInput, mq)
  }

  private def getProjectUpsertKeys(
      projects: JList[RexNode],
      input: RelNode,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] =
    FlinkRelMdUniqueKeys.INSTANCE.getProjectUniqueKeys(
      projects,
      input.getCluster.getTypeFactory,
      () => FlinkRelMetadataQuery.reuseOrCreate(mq).getUpsertKeys(input),
      ignoreNulls = false)

  def getUpsertKeys(rel: Expand, mq: RelMetadataQuery): JSet[ImmutableBitSet] =
    FlinkRelMdUniqueKeys.INSTANCE.getExpandUniqueKeys(
      rel,
      () => FlinkRelMetadataQuery.reuseOrCreate(mq).getUpsertKeys(rel.getInput))

  def getUpsertKeys(rel: Exchange, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    val keys = FlinkRelMetadataQuery.reuseOrCreate(mq).getUpsertKeys(rel.getInput)
    rel.getDistribution.getType match {
      case RelDistribution.Type.HASH_DISTRIBUTED =>
        filterKeys(keys, ImmutableBitSet.of(rel.getDistribution.getKeys))
      case RelDistribution.Type.SINGLETON => keys
      case t => throw new UnsupportedOperationException("Unsupported distribution type: " + t)
    }
  }

  def getUpsertKeys(rel: Rank, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    rel match {
      case rank: StreamPhysicalRank if RankUtil.isDeduplication(rel) =>
        ImmutableSet.of(ImmutableBitSet.of(rank.partitionKey.toArray.map(Integer.valueOf).toList))
      case _ =>
        val inputKeys = filterKeys(
          FlinkRelMetadataQuery
            .reuseOrCreate(mq)
            .getUpsertKeys(rel.getInput),
          rel.partitionKey)
        FlinkRelMdUniqueKeys.INSTANCE.getRankUniqueKeys(rel, inputKeys)
    }
  }

  def getUpsertKeys(rel: Sort, mq: RelMetadataQuery): JSet[ImmutableBitSet] =
    filterKeys(
      FlinkRelMetadataQuery.reuseOrCreate(mq).getUpsertKeys(rel.getInput),
      ImmutableBitSet.of(rel.getCollation.getKeys))

  def getUpsertKeys(
      rel: StreamPhysicalChangelogNormalize,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    ImmutableSet.of(ImmutableBitSet.of(rel.uniqueKeys.map(Integer.valueOf).toList))
  }

  def getUpsertKeys(
      rel: StreamPhysicalMiniBatchAssigner,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMetadataQuery.reuseOrCreate(mq).getUpsertKeys(rel.getInput)
  }

  def getUpsertKeys(
      rel: StreamPhysicalDropUpdateBefore,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMetadataQuery.reuseOrCreate(mq).getUpsertKeys(rel.getInput)
  }

  def getUpsertKeys(rel: Aggregate, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnAggregate(rel.getGroupSet.toArray)
  }

  def getUpsertKeys(
      rel: BatchPhysicalGroupAggregateBase,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    if (rel.isFinal) {
      FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnAggregate(rel.grouping)
    } else {
      null
    }
  }

  def getUpsertKeys(
      rel: StreamPhysicalGroupAggregate,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnAggregate(rel.grouping)
  }

  def getUpsertKeys(
      rel: StreamPhysicalLocalGroupAggregate,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] = null

  def getUpsertKeys(
      rel: StreamPhysicalGlobalGroupAggregate,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnAggregate(rel.grouping)
  }

  def getUpsertKeys(rel: WindowAggregate, mq: RelMetadataQuery): util.Set[ImmutableBitSet] = {
    FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnWindowAgg(
      rel.getRowType.getFieldCount,
      rel.getNamedProperties,
      rel.getGroupSet.toArray)
  }

  def getUpsertKeys(
      rel: BatchPhysicalWindowAggregateBase,
      mq: RelMetadataQuery): util.Set[ImmutableBitSet] = {
    if (rel.isFinal) {
      FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnWindowAgg(
        rel.getRowType.getFieldCount,
        rel.namedWindowProperties,
        rel.grouping)
    } else {
      null
    }
  }

  def getUpsertKeys(
      rel: StreamPhysicalGroupWindowAggregate,
      mq: RelMetadataQuery): util.Set[ImmutableBitSet] = {
    FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnWindowAgg(
      rel.getRowType.getFieldCount,
      rel.namedWindowProperties,
      rel.grouping)
  }

  def getUpsertKeys(rel: Window, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    getUpsertKeysOnOver(rel, mq, rel.groups.map(_.keys): _*)
  }

  def getUpsertKeys(
      rel: BatchPhysicalOverAggregate,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    getUpsertKeysOnOver(rel, mq, ImmutableBitSet.of(rel.partitionKeyIndices: _*))
  }

  def getUpsertKeys(
      rel: StreamPhysicalOverAggregate,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    getUpsertKeysOnOver(rel, mq, rel.logicWindow.groups.map(_.keys): _*)
  }

  private def getUpsertKeysOnOver(
      rel: SingleRel,
      mq: RelMetadataQuery,
      distributionKeys: ImmutableBitSet*): JSet[ImmutableBitSet] = {
    var inputKeys = FlinkRelMetadataQuery.reuseOrCreate(mq).getUpsertKeys(rel.getInput)
    for (distributionKey <- distributionKeys) {
      inputKeys = filterKeys(inputKeys, distributionKey)
    }
    inputKeys
  }

  def getUpsertKeys(join: Join, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    val joinInfo = join.analyzeCondition()
    join.getJoinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        filterKeys(
          FlinkRelMetadataQuery.reuseOrCreate(mq).getUpsertKeys(join.getLeft),
          joinInfo.leftSet())
      case _ =>
        getJoinUpsertKeys(joinInfo, join.getJoinType, join.getLeft, join.getRight, mq)
    }
  }

  def getUpsertKeys(
      rel: StreamPhysicalIntervalJoin,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    val joinInfo = JoinInfo.of(rel.getLeft, rel.getRight, rel.originalCondition)
    getJoinUpsertKeys(joinInfo, rel.getJoinType, rel.getLeft, rel.getRight, mq)
  }

  def getUpsertKeys(
      rel: StreamPhysicalProcessTableFunction,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMdUniqueKeys.INSTANCE.getPtfUniqueKeys(rel)
  }

  def getUpsertKeys(
      join: CommonPhysicalLookupJoin,
      mq: RelMetadataQuery): util.Set[ImmutableBitSet] = {
    val left = join.getInput
    val leftType = left.getRowType
    val leftJoinKeys = join.joinInfo.leftSet
    // differs from regular join, here we do not filterKeys because there's no shuffle on join keys
    // by default.
    val leftUpsertKeys = FlinkRelMetadataQuery.reuseOrCreate(mq).getUpsertKeys(left)
    val rightUniqueKeys = FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOfTemporalTable(join)

    val remainingConditionNonDeterministic =
      join.finalPreFilterCondition.exists(c => !RexUtil.isDeterministic(c)) ||
        join.finalRemainingCondition.exists(c => !RexUtil.isDeterministic(c))
    lazy val calcOnTemporalTableNonDeterministic =
      join.calcOnTemporalTable.exists(p => !FlinkRexUtil.isDeterministic(p))

    val rightUpsertKeys =
      if (remainingConditionNonDeterministic || calcOnTemporalTableNonDeterministic) {
        null
      } else {
        rightUniqueKeys
      }

    FlinkRelMdUniqueKeys.INSTANCE.getJoinUniqueKeys(
      join.joinType,
      leftType,
      leftUpsertKeys,
      rightUpsertKeys,
      areColumnsUpsertKeys(leftUpsertKeys, leftJoinKeys),
      rightUpsertKeys != null
    )
  }

  private def getJoinUpsertKeys(
      joinInfo: JoinInfo,
      joinRelType: JoinRelType,
      left: RelNode,
      right: RelNode,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val leftKeys = fmq.getUpsertKeys(left)
    val rightKeys = fmq.getUpsertKeys(right)
    FlinkRelMdUniqueKeys.INSTANCE.getJoinUniqueKeys(
      joinRelType,
      left.getRowType,
      filterKeys(leftKeys, joinInfo.leftSet),
      filterKeys(rightKeys, joinInfo.rightSet),
      areColumnsUpsertKeys(leftKeys, joinInfo.leftSet),
      areColumnsUpsertKeys(rightKeys, joinInfo.rightSet)
    )
  }

  def getUpsertKeys(rel: SetOp, mq: RelMetadataQuery): JSet[ImmutableBitSet] =
    FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeys(rel, mq, ignoreNulls = false)

  def getUpsertKeys(subset: RelSubset, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    if (!Bug.CALCITE_1048_FIXED) {
      // if the best node is null, so we can get the uniqueKeys based original node, due to
      // the original node is logically equivalent as the rel.
      val rel = Util.first(subset.getBest, subset.getOriginal)
      FlinkRelMetadataQuery.reuseOrCreate(mq).getUpsertKeys(rel)
    } else {
      throw new RuntimeException("CALCITE_1048 is fixed, so check this method again!")
    }
  }

  def getUpsertKeys(subset: HepRelVertex, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMetadataQuery.reuseOrCreate(mq).getUpsertKeys(subset.getCurrentRel)
  }

  def getUpsertKeys(subset: WatermarkAssigner, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMetadataQuery.reuseOrCreate(mq).getUpsertKeys(subset.getInput)
  }

  private def filterKeys(
      keys: JSet[ImmutableBitSet],
      distributionKey: ImmutableBitSet): JSet[ImmutableBitSet] = {
    if (keys != null) {
      keys.filter(k => k.contains(distributionKey))
    } else {
      null
    }
  }

  private def areColumnsUpsertKeys(
      keys: JSet[ImmutableBitSet],
      columns: ImmutableBitSet): Boolean = {
    if (keys != null) {
      keys.exists(columns.contains)
    } else {
      false
    }
  }

  // Catch-all rule when none of the others apply.
  def getUpsertKeys(rel: RelNode, mq: RelMetadataQuery): JSet[ImmutableBitSet] = null
}

object FlinkRelMdUpsertKeys {

  private val INSTANCE = new FlinkRelMdUpsertKeys

  val SOURCE: RelMetadataProvider =
    ReflectiveRelMetadataProvider.reflectiveSource(UpsertKeys.METHOD, INSTANCE)

}
