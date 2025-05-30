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

package org.apache.flink.api.common.state.v2;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link StateDescriptor} for {@link org.apache.flink.api.common.state.v2.AggregatingState}.
 *
 * <p>The type internally stored in the state is the type of the {@code Accumulator} of the {@code
 * AggregateFunction}.
 *
 * @param <IN> The type of the values that are added to the state.
 * @param <ACC> The type of the accumulator (intermediate aggregation state).
 * @param <OUT> The type of the values that are returned from the state.
 */
@Experimental
public class AggregatingStateDescriptor<IN, ACC, OUT> extends StateDescriptor<ACC> {

    private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

    /**
     * Create a new {@code AggregatingStateDescriptor} with the given name, function, and type.
     *
     * @param stateId The (unique) name for the state.
     * @param aggregateFunction The {@code AggregateFunction} used to aggregate the state.
     * @param typeInfo The type of the accumulator. The accumulator is stored in the state.
     */
    public AggregatingStateDescriptor(
            @Nonnull String stateId,
            @Nonnull AggregateFunction<IN, ACC, OUT> aggregateFunction,
            @Nonnull TypeInformation<ACC> typeInfo) {
        super(stateId, typeInfo);
        this.aggregateFunction = checkNotNull(aggregateFunction);
    }

    /**
     * Create a new {@code AggregatingStateDescriptor} with the given stateId and the given type
     * serializer.
     *
     * @param stateId The (unique) stateId for the state.
     * @param serializer The type serializer for accumulator.
     */
    public AggregatingStateDescriptor(
            @Nonnull String stateId,
            @Nonnull AggregateFunction<IN, ACC, OUT> aggregateFunction,
            @Nonnull TypeSerializer<ACC> serializer) {
        super(stateId, serializer);
        this.aggregateFunction = checkNotNull(aggregateFunction);
    }

    /**
     * Creates a new {@code AggregatingStateDescriptor} with the given name, function, and type.
     *
     * <p>If this constructor fails (because it is not possible to describe the type via a class),
     * consider using the {@link #AggregatingStateDescriptor(String, AggregateFunction,
     * TypeInformation)} constructor.
     *
     * @param name The (unique) name for the state.
     * @param aggFunction The {@code AggregateFunction} used to aggregate the state.
     * @param stateType The type of the accumulator. The accumulator is stored in the state.
     */
    public AggregatingStateDescriptor(
            String name, AggregateFunction<IN, ACC, OUT> aggFunction, Class<ACC> stateType) {
        super(name, stateType);
        this.aggregateFunction = checkNotNull(aggFunction);
    }

    /** Returns the Aggregate function for this state. */
    public AggregateFunction<IN, ACC, OUT> getAggregateFunction() {
        return aggregateFunction;
    }

    @Override
    public Type getType() {
        return Type.AGGREGATING;
    }
}
