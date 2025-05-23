---
title: "Working with State"
weight: 1
type: docs
aliases:
  - /zh/dev/stream/state/state.html
  - /zh/apis/streaming/state.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# 使用状态

本章节您将了解 Flink 用于编写有状态程序的 API。要了解有状态流处理背后的概念，请参阅[Stateful Stream
Processing]({{< ref "docs/concepts/stateful-stream-processing" >}})。

## Keyed DataStream

如果你希望使用 keyed state，首先需要为`DataStream`指定 key（主键）。这个主键用于状态分区（也会给数据流中的记录本身分区）。
你可以使用 `DataStream` 中 Java API 的 `keyBy(KeySelector)` 或者是 Python API 的 `key_by(KeySelector)` 来指定 key。
它将生成 `KeyedStream`，接下来允许使用 keyed state 操作。

Key selector 函数接收单条记录作为输入，返回这条记录的 key。该 key 可以为任何类型，但是它的计算产生方式**必须**是具备确定性的。

Flink 的数据模型不基于 key-value 对，因此实际上将数据集在物理上封装成 key 和 value 是没有必要的。
Key 是“虚拟”的。它们定义为基于实际数据的函数，用以操纵分组算子。

下面的例子展示了 key selector 函数。它仅返回了对象当中的字段。

{{< tabs "9730828c-2f0f-48c8-9a5c-4ec415d0c492" >}}
{{< tab "Java" >}}
```java
// some ordinary POJO
public class WC {
  public String word;
  public int count;

  public String getWord() { return word; }
}
DataStream<WC> words = // [...]
KeyedStream<WC> keyed = words
  .keyBy(WC::getWord);
```

{{< /tab >}}
{{< tab "Python" >}}
```python
words = # type: DataStream[Row]
keyed = words.key_by(lambda row: row[0])
```
{{< /tab >}}
{{< /tabs >}}

#### Tuple Keys 和 Expression Keys

Flink 也有两种不同定义 key 的方式：Java API（Python API 仍未支持） 的 Tuple key（通过字段索引指定的 key）和 Expression key（通过字段名称指定的 key）。
借此你可以通过 tuple 字段索引，或者是选取对象字段的表达式来指定 key。
如今我们不建议这样使用，但你可以参考 `DataStream` 的 Javadoc 来了解它们。 
使用 KeySelector 函数显然是更好的。以几乎可以忽略的额外开销为代价，结合 Java Lambda 表达式，我们可以更方便得使用KeySelector。

{{< top >}}

## 使用 Keyed State

keyed state 接口提供不同类型状态的访问接口，这些状态都作用于当前输入数据的 key 下。换句话说，这些状态仅可在 `KeyedStream`
上使用，在Java API上可以通过 `stream.keyBy(...)` 得到 `KeyedStream`，在Python API上可以通过 `stream.key_by(...)` 得到 `KeyedStream`。

接下来，我们会介绍不同类型的状态，然后介绍如何使用他们。所有支持的状态类型如下所示：

* `ValueState<T>`: 保存一个可以更新和检索的值（如上所述，每个值都对应到当前的输入数据的 key，因此算子接收到的每个 key 都可能对应一个值）。
这个值可以通过 `update(T)` 进行更新，通过 `T value()` 进行检索。


* `ListState<T>`: 保存一个元素的列表。可以往这个列表中追加数据，并在当前的列表上进行检索。可以通过
 `add(T)` 或者 `addAll(List<T>)` 进行添加元素，通过 `Iterable<T> get()` 获得整个列表。还可以通过 `update(List<T>)` 覆盖当前的列表。

* `ReducingState<T>`: 保存一个单值，表示添加到状态的所有值的聚合。接口与 `ListState` 类似，但使用 `add(T)` 增加元素，会使用提供的 `ReduceFunction` 进行聚合。

* `AggregatingState<IN, OUT>`: 保留一个单值，表示添加到状态的所有值的聚合。和 `ReducingState` 相反的是, 聚合类型可能与 添加到状态的元素的类型不同。
接口与 `ListState` 类似，但使用 `add(IN)` 添加的元素会用指定的 `AggregateFunction` 进行聚合。

* `MapState<UK, UV>`: 维护了一个映射列表。 你可以添加键值对到状态中，也可以获得反映当前所有映射的迭代器。使用 `put(UK，UV)` 或者 `putAll(Map<UK，UV>)` 添加映射。
 使用 `get(UK)` 检索特定 key。 使用 `entries()`，`keys()` 和 `values()` 分别检索映射、键和值的可迭代视图。你还可以通过 `isEmpty()` 来判断是否包含任何键值对。

所有类型的状态还有一个`clear()` 方法，清除当前 key 下的状态数据，也就是当前输入元素的 key。

请牢记，这些状态对象仅用于与状态交互。状态本身不一定存储在内存中，还可能在磁盘或其他位置。
另外需要牢记的是从状态中获取的值取决于输入元素所代表的 key。 因此，在不同 key 上调用同一个接口，可能得到不同的值。

你必须创建一个 `StateDescriptor`，才能得到对应的状态句柄。 这保存了状态名称（正如我们稍后将看到的，你可以创建多个状态，并且它们必须具有唯一的名称以便可以引用它们），
状态所持有值的类型，并且可能包含用户指定的函数，例如`ReduceFunction`。 根据不同的状态类型，可以创建`ValueStateDescriptor`，`ListStateDescriptor`，
`AggregatingStateDescriptor`, `ReducingStateDescriptor` 或 `MapStateDescriptor`。

状态通过 `RuntimeContext` 进行访问，因此只能在 *rich functions* 中使用。请参阅[这里]({{< ref "docs/dev/datastream/user_defined_functions" >}}#rich-functions)获取相关信息，
但是我们很快也会看到一个例子。`RichFunction` 中 `RuntimeContext` 提供如下方法：

* `ValueState<T> getState(ValueStateDescriptor<T>)`
* `ReducingState<T> getReducingState(ReducingStateDescriptor<T>)`
* `ListState<T> getListState(ListStateDescriptor<T>)`
* `AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT>)`
* `MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV>)`

下面是一个 `FlatMapFunction` 的例子，展示了如何将这些部分组合起来：

{{< tabs "76a14a59-71da-4619-a2f8-463a58515e5e" >}}
{{< tab "Java" >}}
```java
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

        // access the state value
        Tuple2<Long, Long> currentSum = sum.value();

        // update the count
        currentSum.f0 += 1;

        // add the second field of the input value
        currentSum.f1 += input.f1;

        // update the state
        sum.update(currentSum);

        // if the count reaches 2, emit the average and clear the state
        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(OpenContext ctx) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                        Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
    }
}

// this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
        .keyBy(value -> value.f0)
        .flatMap(new CountWindowAverage())
        .print();

// the printed output will be (1,4) and (1,5)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, FlatMapFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

class CountWindowAverage(FlatMapFunction):

    def __init__(self):
        self.sum = None

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor(
            "average",  # the state name
            Types.PICKLED_BYTE_ARRAY()  # type information
        )
        self.sum = runtime_context.get_state(descriptor)

    def flat_map(self, value):
        # access the state value
        current_sum = self.sum.value()
        if current_sum is None:
            current_sum = (0, 0)

        # update the count
        current_sum = (current_sum[0] + 1, current_sum[1] + value[1])

        # update the state
        self.sum.update(current_sum)

        # if the count reaches 2, emit the average and clear the state
        if current_sum[0] >= 2:
            self.sum.clear()
            yield value[0], int(current_sum[1] / current_sum[0])


env = StreamExecutionEnvironment.get_execution_environment()
env.from_collection([(1, 3), (1, 5), (1, 7), (1, 4), (1, 2)]) \
    .key_by(lambda row: row[0]) \
    .flat_map(CountWindowAverage()) \
    .print()

env.execute()

# the printed output will be (1,4) and (1,5)
```
{{< /tab >}}
{{< /tabs >}}

这个例子实现了一个简单的计数窗口。 我们把元组的第一个元素当作 key（在示例中都 key 都是 "1"）。 该函数将出现的次数以及总和存储在 "ValueState" 中。 
一旦出现次数达到 2，则将平均值发送到下游，并清除状态重新开始。 请注意，我们会为每个不同的 key（元组中第一个元素）保存一个单独的值。

### 状态有效期 (TTL)

任何类型的 keyed state 都可以有 *有效期* (TTL)。如果配置了 TTL 且状态值已过期，则会尽最大可能清除对应的值，这会在后面详述。

所有状态类型都支持单元素的 TTL。 这意味着列表元素和映射元素将独立到期。

在使用状态 TTL 前，需要先构建一个`StateTtlConfig` 配置对象。 然后把配置传递到 state descriptor 中启用 TTL 功能：

{{< tabs "b1c41e38-ec86-4c56-a6f6-de5c5817bd6c" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import java.time.Duration;

StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Duration.ofSeconds(1))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();
    
ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("text state", String.class);
stateDescriptor.enableTimeToLive(ttlConfig);
```
{{< /tab >}}
{{< /tabs >}}

TTL 配置有以下几个选项：
`newBuilder` 的第一个参数表示数据的有效期，是必选项。

TTL 的更新策略（默认是 `OnCreateAndWrite`）：

 - `StateTtlConfig.UpdateType.OnCreateAndWrite` - 仅在创建和写入时更新
 - `StateTtlConfig.UpdateType.OnReadAndWrite` - 读取时也更新

    (**注意:** 如果你同时将状态的可见性配置为 `StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp`，
    那么在PyFlink作业中，状态的读缓存将会失效，这将导致一部分的性能损失)
 
数据在过期但还未被清理时的可见性配置如下（默认为 `NeverReturnExpired`):

 - `StateTtlConfig.StateVisibility.NeverReturnExpired` - 不返回过期数据

    (**注意:** 在PyFlink作业中，状态的读写缓存都将失效，这将导致一部分的性能损失)

 - `StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp` - 会返回过期但未清理的数据
 
`NeverReturnExpired` 情况下，过期数据就像不存在一样，不管是否被物理删除。这对于不能访问过期数据的场景下非常有用，比如敏感数据。
`ReturnExpiredIfNotCleanedUp` 在数据被物理删除前都会返回。

**注意:** 

- 状态上次的修改时间会和数据一起保存在 state backend 中，因此开启该特性会增加状态数据的存储。
Heap state backend 会额外存储一个包括用户状态以及时间戳的 Java 对象，RocksDB state backend 会在每个状态值（list 或者 map 的每个元素）序列化后增加 8 个字节。 

- 暂时只支持基于 *processing time* 的 TTL。

- 尝试从 checkpoint/savepoint 进行恢复时，TTL 的状态（是否开启）必须和之前保持一致，否则会遇到 "StateMigrationException"。

- TTL 的配置并不会保存在 checkpoint/savepoint 中，仅对当前 Job 有效。

- 不建议checkpoint恢复前后将state TTL从短调长，这可能会产生潜在的数据错误。

- 当前开启 TTL 的 map state 仅在用户值序列化器支持 null 的情况下，才支持用户值为 null。如果用户值序列化器不支持 null，
可以用 `NullableSerializer` 包装一层。
  
- 启用 TTL 配置后，`StateDescriptor` 中的 `defaultValue`（已被标记 `deprecated`）将会失效。这个设计的目的是为了确保语义更加清晰，在此基础上，用户需要手动管理那些实际值为 null 或已过期的状态默认值。

#### 过期数据的清理

默认情况下，过期数据会在读取的时候被删除，例如 `ValueState#value`，同时会有后台线程定期清理（如果 StateBackend 支持的话）。可以通过 `StateTtlConfig` 配置关闭后台清理：


{{< tabs "99c1d874-3d6d-41d9-b58a-bda678fedc70" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.state.StateTtlConfig;

StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Duration.ofSeconds(1))
    .disableCleanupInBackground()
    .build();
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common.time import Time
from pyflink.datastream.state import StateTtlConfig

ttl_config = StateTtlConfig \
  .new_builder(Duration.ofSeconds(1)) \
  .disable_cleanup_in_background() \
  .build()
```
{{< /tab >}}
{{< /tabs >}}

可以按照如下所示配置更细粒度的后台清理策略。当前的实现中 `HeapStateBackend` 依赖增量数据清理，`RocksDBStateBackend` 利用压缩过滤器进行后台清理。

#### 全量快照时进行清理

另外，你可以启用全量快照时进行清理的策略，这可以减少整个快照的大小。当前实现中不会清理本地的状态，但从上次快照恢复时，不会恢复那些已经删除的过期数据。
该策略可以通过 `StateTtlConfig` 配置进行配置：

{{< tabs "77959bcd-25cb-476a-893f-53424a723f0e" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.state.StateTtlConfig;
import java.time.Duration;

StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Duration.ofSeconds(1))
    .cleanupFullSnapshot()
    .build();
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common.time import Time
from pyflink.datastream.state import StateTtlConfig

ttl_config = StateTtlConfig \
  .new_builder(Duration.ofSeconds(1)) \
  .cleanup_full_snapshot() \
  .build()
```
{{< /tab >}}
{{< /tabs >}}

这种策略在 `RocksDBStateBackend` 的增量 checkpoint 模式下无效。

**注意:**
- 这种清理方式可以在任何时候通过 `StateTtlConfig` 启用或者关闭，比如在从 savepoint 恢复时。

##### 增量数据清理

另外可以选择增量式清理状态数据，在状态访问或/和处理时进行。如果某个状态开启了该清理策略，则会在存储后端保留一个所有状态的惰性全局迭代器。
每次触发增量清理时，从迭代器中选择已经过期的数进行清理。

该特性可以通过 `StateTtlConfig` 进行配置：

{{< tabs "97f3b853-06df-43c6-a4a1-b50c796bdb52" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.state.StateTtlConfig;
 StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Duration.ofSeconds(1))
    .cleanupIncrementally(10, true)
    .build();
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common.time import Time
from pyflink.datastream.state import StateTtlConfig

ttl_config = StateTtlConfig \
  .new_builder(Duration.ofSeconds(1)) \
  .cleanup_incrementally(10, True) \
  .build()
```
{{< /tab >}}
{{< /tabs >}}

该策略有两个参数。 第一个是每次清理时检查状态的条目数，在每个状态访问时触发。第二个参数表示是否在处理每条记录时触发清理。
Heap backend 默认会检查 5 条状态，并且关闭在每条记录时触发清理。

**注意:**
- 如果没有 state 访问，也没有处理数据，则不会清理过期数据。
- 增量清理会增加数据处理的耗时。
- 现在仅 Heap state backend 支持增量清除机制。在 RocksDB state backend 上启用该特性无效。
- 如果 Heap state backend 使用同步快照方式，则会保存一份所有 key 的拷贝，从而防止并发修改问题，因此会增加内存的使用。但异步快照则没有这个问题。
- 对已有的作业，这个清理方式可以在任何时候通过 `StateTtlConfig` 启用或禁用该特性，比如从 savepoint 重启后。

##### 在 RocksDB 压缩时清理

如果使用 RocksDB state backend，则会启用 Flink 为 RocksDB 定制的压缩过滤器。RocksDB 会周期性的对数据进行合并压缩从而减少存储空间。
Flink 提供的 RocksDB 压缩过滤器会在压缩时过滤掉已经过期的状态数据。

该特性可以通过 `StateTtlConfig` 进行配置：

{{< tabs "1a8a996b-f030-4e0d-9e76-1df6ee3006a1" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.state.StateTtlConfig;

StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Duration.ofSeconds(1))
    .cleanupInRocksdbCompactFilter(1000, Duration.ofHours(1))
    .build();
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common import Duration
from pyflink.common.time import Time
from pyflink.datastream.state import StateTtlConfig

ttl_config = StateTtlConfig \
  .new_builder(Time.seconds(1)) \
  .cleanup_in_rocksdb_compact_filter(1000, Duration.of_hours(1)) \
  .build()
```
{{< /tab >}}
{{< /tabs >}}

Flink 处理一定条数的状态数据后，会使用当前时间戳来检测 RocksDB 中的状态是否已经过期，
你可以通过 `StateTtlConfig.newBuilder(...).cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries)` 方法指定处理状态的条数。
时间戳更新的越频繁，状态的清理越及时，但由于压缩会有调用 JNI 的开销，因此会影响整体的压缩性能。
RocksDB backend 的默认后台清理策略会每处理 1000 条数据进行一次。

定期压缩可以加速过期状态条目的清理，特别是对于很少访问的状态条目。
比这个值早的文件将被选取进行压缩，并重新写入与之前相同的 Level 中。 
该功能可以确保文件定期通过压缩过滤器压缩。
您可以通过`StateTtlConfig.newBuilder(...).cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries, Duration periodicCompactionTime)` 
方法设定定期压缩的时间。
定期压缩的时间的默认值是 30 天。
您可以将其设置为 0 以关闭定期压缩或设置一个较小的值以加速过期状态条目的清理，但它将会触发更多压缩。

你还可以通过配置开启 RocksDB 过滤器的 debug 日志：
`log4j.logger.org.rocksdb.FlinkCompactionFilter=DEBUG`

**注意:**
- 压缩时调用 TTL 过滤器会降低速度。TTL 过滤器需要解析上次访问的时间戳，并对每个将参与压缩的状态进行是否过期检查。
对于集合型状态类型（比如 list 和 map），会对集合中每个元素进行检查。
- 对于元素序列化后长度不固定的列表状态，TTL 过滤器需要在每次 JNI 调用过程中，额外调用 Flink 的 java 序列化器，
从而确定下一个未过期数据的位置。
- 对已有的作业，这个清理方式可以在任何时候通过 `StateTtlConfig` 启用或禁用该特性，比如从 savepoint 重启后。
- 定期压缩功能只在 TTL 启用时生效。

## 算子状态 (Operator State)

*算子状态*（或者*非 keyed 状态*）是绑定到一个并行算子实例的状态。[Kafka Connector]({{< ref "docs/connectors/datastream/kafka" >}}) 是 Flink 中使用算子状态一个很具有启发性的例子。Kafka consumer 每个并行实例维护了 topic partitions 和偏移量的 map 作为它的算子状态。

当并行度改变的时候，算子状态支持将状态重新分发给各并行算子实例。处理重分发过程有多种不同的方案。

在典型的有状态 Flink 应用中你无需使用算子状态。它大都作为一种特殊类型的状态使用。用于实现 source/sink，以及无法对 state 进行分区而没有主键的这类场景中。

**注意：** Python DataStream API 仍无法支持算子状态。

## 广播状态 (Broadcast State)

*广播状态*是一种特殊的*算子状态*。引入它的目的在于支持一个流中的元素需要广播到所有下游任务的使用情形。在这些任务中广播状态用于保持所有子任务状态相同。
该状态接下来可在第二个处理记录的数据流中访问。可以设想包含了一系列用于处理其他流中元素规则的低吞吐量数据流，这个例子自然而然地运用了广播状态。
考虑到上述这类使用情形，广播状态和其他算子状态的不同之处在于：

 1. 它具有 map 格式，
 2. 它仅在一些特殊的算子中可用。这些算子的输入为一个*广播*数据流和*非广播*数据流，
 3. 这类算子可以拥有不同命名的*多个广播状态* 。

{{< top >}}

## 使用 Operator State

用户可以通过实现 `CheckpointedFunction` 接口来使用 operator state。

#### CheckpointedFunction

`CheckpointedFunction` 接口提供了访问 non-keyed state 的方法，需要实现如下两个方法：

```java
void snapshotState(FunctionSnapshotContext context) throws Exception;

void initializeState(FunctionInitializationContext context) throws Exception;
```

进行 checkpoint 时会调用 `snapshotState()`。 用户自定义函数初始化时会调用 `initializeState()`，初始化包括第一次自定义函数初始化和从之前的 checkpoint 恢复。
因此 `initializeState()` 不仅是定义不同状态类型初始化的地方，也需要包括状态恢复的逻辑。

当前 operator state 以 list 的形式存在。这些状态是一个 *可序列化* 对象的集合 `List`，彼此独立，方便在改变并发后进行状态的重新分派。
换句话说，这些对象是重新分配 non-keyed state 的最细粒度。根据状态的不同访问方式，有如下几种重新分配的模式：

  - **Even-split redistribution:** 每个算子都保存一个列表形式的状态集合，整个状态由所有的列表拼接而成。当作业恢复或重新分配的时候，整个状态会按照算子的并发度进行均匀分配。
    比如说，算子 A 的并发读为 1，包含两个元素 `element1` 和 `element2`，当并发读增加为 2 时，`element1` 会被分到并发 0 上，`element2` 则会被分到并发 1 上。

  - **Union redistribution:** 每个算子保存一个列表形式的状态集合。整个状态由所有的列表拼接而成。当作业恢复或重新分配时，每个算子都将获得所有的状态数据。
    Do not use this feature if your list may have high cardinality. Checkpoint metadata will store an offset to each list entry, which could lead to RPC framesize or out-of-memory errors.

下面的例子中的 `SinkFunction` 在 `CheckpointedFunction` 中进行数据缓存，然后统一发送到下游，这个例子演示了列表状态数据的 event-split redistribution。 

{{< tabs "03fecab3-b48b-4d06-86ed-8769708ae7ca" >}}
{{< tab "Java" >}}
```java
public class BufferingSink
        implements SinkFunction<Tuple2<String, Integer>>,
                   CheckpointedFunction {

    private final int threshold;

    private transient ListState<Tuple2<String, Integer>> checkpointedState;

    private List<Tuple2<String, Integer>> bufferedElements;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context contex) throws Exception {
        bufferedElements.add(value);
        if (bufferedElements.size() >= threshold) {
            for (Tuple2<String, Integer> element: bufferedElements) {
                // send it to the sink
            }
            bufferedElements.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.update(bufferedElements);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
            new ListStateDescriptor<>(
                "buffered-elements",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }
}
```
{{< /tab >}}
{{< /tabs >}}

`initializeState` 方法接收一个 `FunctionInitializationContext` 参数，会用来初始化 non-keyed state 的 "容器"。这些容器是一个 `ListState`
用于在 checkpoint 时保存 non-keyed state 对象。

注意这些状态是如何初始化的，和 keyed state 类似，`StateDescriptor` 会包括状态名字、以及状态类型相关信息。


{{< tabs "9f372f5f-ad80-4b2c-a318-fcbdb19c7d2a" >}}
{{< tab "Java" >}}
```java
ListStateDescriptor<Tuple2<String, Integer>> descriptor =
    new ListStateDescriptor<>(
        "buffered-elements",
        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

checkpointedState = context.getOperatorStateStore().getListState(descriptor);
```

{{< /tab >}}
{{< /tabs >}}

调用不同的获取状态对象的接口，会使用不同的状态分配算法。比如 `getUnionListState(descriptor)` 会使用 union redistribution 算法，
而 `getListState(descriptor)` 则简单的使用 even-split redistribution 算法。

当初始化好状态对象后，我们通过 `isRestored()` 方法判断是否从之前的故障中恢复回来，如果该方法返回 `true` 则表示从故障中进行恢复，会执行接下来的恢复逻辑。

正如代码所示，`BufferingSink` 中初始化时，恢复回来的 `ListState` 的所有元素会添加到一个局部变量中，供下次 `snapshotState()` 时使用。
然后清空 `ListState`，再把当前局部变量中的所有元素写入到 checkpoint 中。

另外，我们同样可以在 `initializeState()` 方法中使用 `FunctionInitializationContext` 初始化 keyed state。

### 带状态的 Source Function

带状态的数据源比其他的算子需要注意更多东西。为了保证更新状态以及输出的原子性（用于支持 exactly-once 语义），用户需要在发送数据前获取数据源的全局锁。

{{< tabs "0d664c7a-c695-4306-b562-e0cb36ae9efa" >}}
{{< tab "Java" >}}
```java
public static class CounterSource
        extends RichParallelSourceFunction<Long>
        implements CheckpointedFunction {

    /**  current offset for exactly once semantics */
    private Long offset = 0L;

    /** flag for job cancellation */
    private volatile boolean isRunning = true;
    
    /** 存储 state 的变量. */
    private ListState<Long> state;
     
    @Override
    public void run(SourceContext<Long> ctx) {
        final Object lock = ctx.getCheckpointLock();

        while (isRunning) {
            // output and state update are atomic
            synchronized (lock) {
                ctx.collect(offset);
                offset += 1;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
            "state",
            LongSerializer.INSTANCE));
            
        // 从我们已保存的状态中恢复 offset 到内存中，在进行任务恢复的时候也会调用此初始化状态的方法
        for (Long l : state.get()) {
            offset = l;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        state.update(Collections.singletonList(offset));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

希望订阅 checkpoint 成功消息的算子，可以参考 `org.apache.flink.api.common.state.CheckpointListener` 接口。

{{< top >}}
