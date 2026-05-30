# SparkAdvisor 规则目录

本文档是规则说明书，供开发、运维和调优排查时查阅。运行时规则仍以 Java `RuleEngine` 为准；本文档不作为运行时配置源。

规则 ID、证据字段、枚举值和 Spark 参数名保留英文原文，便于和 JSON 契约、CLI/UI 报告以及 Spark 配置对应。

## R1_DATA_SKEW：数据倾斜

命中条件：

- Stage 的 Task 耗时 `max / median >= skewRatioWarn`。
- 或 shuffle read 分布 `max / median >= shuffleSkewWarn`。
- 当耗时倾斜超过 `skewRatioCritical` 时升级为 `CRITICAL`。

证据字段：

- `durationSkewRatio`
- `shuffleReadSkewRatio`
- `maxTaskMs`
- `medianTaskMs`

诊断含义：

最慢 Task 远高于中位数 Task，说明 wall clock 被少数长尾 Task 限制。此类 Stage 通常受单分区、热点 key 或倾斜 join/group 影响；单纯增加 Executor 往往无法缩短最长 Task。

建议动作：

- AQE 未开时，开启 `spark.sql.adaptive.enabled=true` 和 `spark.sql.adaptive.skewJoin.enabled=true`。
- AQE 已开但倾斜仍存在时，调低 `spark.sql.adaptive.skewJoin.skewedPartitionFactor` 和/或 `skewedPartitionThresholdInBytes`。
- 已知热点 key 时，对 join/group key 做 salting。

注意事项：

倾斜场景下，优先处理 key 分布或 AQE skew join；不建议把 `spark.sql.shuffle.partitions` 当作第一修复手段。

## R2_EXCESSIVE_SPILL：过量 Spill

命中条件：

- Stage spill bytes 大于 0。
- `spillBytes / max(shuffleReadBytes, 1) >= spillRatioWarn`。

证据字段：

- `spillBytes`
- `shuffleReadBytes`
- `spillRatio`

诊断含义：

分区数据相对单 Task 内存预算过大，导致内存或磁盘 spill。若同一 Stage 同时命中 R1/R6，应优先判断是否为倾斜导致的大分区和 GC。

建议动作：

- AQE coalesce 开启时，优先调低 `spark.sql.adaptive.advisoryPartitionSizeInBytes`。
- 非 AQE 场景可增加 `spark.sql.shuffle.partitions`。
- 适当增加 `spark.executor.memory` / `memoryOverhead`，但这不是根治倾斜的办法。

## R3_LOW_PARALLELISM：并行度不足

命中条件：

- SQL 级 `coreUtilization < coreUtilLow`。
- `coreUtilization > 0`，避免缺失容量数据时误报。

证据字段：

- `coreUtilization`
- `criticalPathMs`
- `idealMs`

诊断含义：

Executor slot 大量空闲，常见原因是分区数过少、AQE coalesce 过度、调度等待或资源获取等待。该规则只说明“并行度利用不足”，需要结合 R8、预测曲线和 Stage 明细判断是否该加资源。

建议动作：

- AQE coalesce 开启时，调低 `spark.sql.adaptive.advisoryPartitionSizeInBytes`，或调高 `coalescePartitions.initialPartitionNum`。
- 非 AQE 场景可增加 `spark.sql.shuffle.partitions` 或对输入 repartition。

## R4_OVER_PARALLELISM：过并行小 Task

命中条件：

- Stage `numTasks >= overParallelMinTasks`。
- Stage `medianTaskMs < smallTaskMedianMs`。

证据字段：

- `numTasks`
- `medianTaskMs`

诊断含义：

Task 很多但单个 Task 很短，调度、启动、反序列化等固定开销可能超过实际计算。此类问题常见于过高分区数、小文件或过度 repartition。

建议动作：

- AQE coalesce 开启时，调高 `spark.sql.adaptive.advisoryPartitionSizeInBytes`。
- 非 AQE 场景可降低 `spark.sql.shuffle.partitions`，或对结果使用 `coalesce()`。

## R5_SMALL_FILES：小文件

命中条件：

- Stage 读取源数据，即 `inputBytes > 0`。
- Stage `numTasks >= overParallelMinTasks`。
- `medianInputBytesPerTask < smallInputPerTaskBytes`。

证据字段：

- `numTasks`
- `medianInputBytesPerTask`
- `totalInputBytes`

诊断含义：

输入文件数量驱动 Task 数，而不是数据量驱动 Task 数。该问题会在每次读取时反复产生调度开销。

建议动作：

- 上游做文件合并、周期性 compaction 或写入时 repartition。
- 调整 `spark.sql.files.maxPartitionBytes` / `openCostInBytes`，让 Spark 每个 scan Task 合并更多小文件。

## R6_GC_PRESSURE：GC 压力

命中条件：

- Stage `gcRatio >= gcRatioWarn`。

证据字段：

- `gcRatio`

诊断含义：

Task 时间中较高比例花在 JVM GC 上，说明对象分配、内存工作集或 UDF 中间对象可能过重。若同时命中 spill，应先处理单 Task 数据量或倾斜。

建议动作：

- 增大 executor memory，或通过更多分区/更小 advisory size 降低单 Task 数据量。
- 检查对象创建较重的 UDF、序列化方式和 GC collector。

## R7_BROADCAST_JOIN：Broadcast Join 机会

命中条件：

- 物理计划包含 `SortMergeJoin`。
- 物理计划不包含 `BroadcastHashJoin` 或 `BroadcastNestedLoopJoin`。

证据字段：

- `planHasSortMergeJoin`
- `planHasBroadcastJoin`

诊断含义：

这是物理计划启发式规则。Event log 不直接告诉我们 broadcast 是否被拒绝，因此该规则只提示“可能有机会”：如果 join 某一侧足够小，broadcast 可以避免一次 shuffle。

建议动作：

- 小表侧可放入内存时，调高 `spark.sql.autoBroadcastJoinThreshold`。
- 明确知道小表侧时，添加 `broadcast()` hint。

注意事项：

必须确认小表侧实际大小。阈值过高可能带来 driver 或 executor 内存风险。

## R8_SCHEDULING_DELAY：调度等待

命中条件：

- Stage wall clock 大于 0。
- `schedulingDelayMs / wallClockMs >= schedulingDelayRatioWarn`。

证据字段：

- `schedulingDelayMs`
- `stageWallMs`
- `delayRatio`

诊断含义：

Stage 从提交到首个 Task 启动之间等待过久，常见于 dynamic allocation 冷启动、资源池排队或调度延迟。

建议动作：

- 对低延迟场景预热 Executor，例如调高 `spark.dynamicAllocation.minExecutors`。
- 固定资源队列下，结合队列报告判断是否存在资源争用。

## R9_SHUFFLE_FETCH_WAIT：Shuffle 拉取等待

命中条件：

- Stage 有 shuffle read。
- `shuffleFetchWaitMs / totalTaskTimeMs >= shuffleFetchWaitRatioWarn`。

证据字段：

- `fetchWaitMs`
- `totalTaskTimeMs`
- `fetchWaitRatio`
- `shuffleRemoteReadBytes`

诊断含义：

Reducer Task 大量时间花在等待 shuffle block，瓶颈更可能在远端 shuffle、网络、shuffle service 或 reducer 分区过大，而不是 CPU 计算。

建议动作：

- 在 Exchange 前减少 shuffle 数据量，例如提前过滤、裁剪列、预聚合。
- 检查 shuffle service、网络、executor locality。
- 如果 reducer 分区过大，增加 reducer 并行度或降低 AQE advisory partition size。

## R10_TASK_RETRY：Task 重试/失败 attempt

命中条件：

- `failedTaskAttempts >= failedTaskAttemptsWarn`。
- 或 `extraTaskAttempts / numTasks >= extraTaskAttemptRatioWarn`。

证据字段：

- `failedTaskAttempts`
- `extraTaskAttempts`
- `extraTaskAttemptRatio`

诊断含义：

Task 重试会放大 wall clock，并降低细粒度调优判断的置信度。需要先确认失败是否由内存、超时、坏节点、数据异常或外部系统抖动导致。

建议动作：

- 检查失败 Task 日志和 executor/container 健康状态。
- 如果失败与大分区、内存或超时相关，降低单 Task 输入或 shuffle 数据量。

## R11_SORT_AGG_SPILL：Sort/Aggregate Spill 归因

命中条件：

- 物理计划包含 `Sort`、`HashAggregate`、`ObjectHashAggregate` 或 `SortAggregate`。
- SQL 的 Stage 总 spill 大于 0。

证据字段：

- `planHasSort`
- `planHasAggregate`
- `totalSpillBytes`

诊断含义：

Event log 的 spill 是 Stage/Task 粒度，不直接标注具体算子。该规则把 spill 与 sort/aggregate-heavy 物理计划做启发式关联，用来提示内存压力可能集中在排序或聚合算子附近。

建议动作：

- 在 sort/aggregate 前减少工作集，例如提前过滤、裁剪列、预聚合。
- 对发生 spill 的 Stage 降低单分区数据量，或增加内存余量。

注意事项：

这是启发式归因，不替代 Spark UI SQL tab 的算子级指标核对。

