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
package org.apache.flink.api.scala.typeutils

import org.apache.flink.annotation.{Public, PublicEvolving}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

import scala.collection.JavaConverters._

/**
 * TypeInformation [[Either]].
 *
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
@Public
class EitherTypeInfo[A, B, T <: Either[A, B]](
    val clazz: Class[T],
    val leftTypeInfo: TypeInformation[A],
    val rightTypeInfo: TypeInformation[B])
  extends TypeInformation[T] {

  @PublicEvolving
  override def isBasicType: Boolean = false
  @PublicEvolving
  override def isTupleType: Boolean = false
  @PublicEvolving
  override def isKeyType: Boolean = false
  @PublicEvolving
  override def getTotalFields: Int = 1
  @PublicEvolving
  override def getArity: Int = 1
  @PublicEvolving
  override def getTypeClass = clazz
  @PublicEvolving
  override def getGenericParameters =
    Map[String, TypeInformation[_]]("A" -> leftTypeInfo, "B" -> rightTypeInfo).asJava

  @PublicEvolving
  override def createSerializer(serializerConfig: SerializerConfig): TypeSerializer[T] = {
    val leftSerializer: TypeSerializer[A] = if (leftTypeInfo != null) {
      leftTypeInfo.createSerializer(serializerConfig)
    } else {
      (new NothingSerializer).asInstanceOf[TypeSerializer[A]]
    }

    val rightSerializer: TypeSerializer[B] = if (rightTypeInfo != null) {
      rightTypeInfo.createSerializer(serializerConfig)
    } else {
      (new NothingSerializer).asInstanceOf[TypeSerializer[B]]
    }
    new EitherSerializer[A, B](leftSerializer, rightSerializer).asInstanceOf[TypeSerializer[T]]
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case eitherTypeInfo: EitherTypeInfo[_, _, _] =>
        eitherTypeInfo.canEqual(this) &&
        clazz.equals(eitherTypeInfo.clazz) &&
        leftTypeInfo.equals(eitherTypeInfo.leftTypeInfo) &&
        rightTypeInfo.equals(eitherTypeInfo.rightTypeInfo)
      case _ => false
    }
  }

  override def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[EitherTypeInfo[_, _, _]]
  }

  override def hashCode(): Int = {
    31 * (31 * clazz.hashCode() + leftTypeInfo.hashCode()) + rightTypeInfo.hashCode()
  }

  override def toString = s"Either[$leftTypeInfo, $rightTypeInfo]"
}
