# SparkAdvisor 规则目录

本文档是规则说明书，供开发、运维和调优排查时查阅。运行时规则仍以 Java `RuleEngine` 为准；本文档不作为运行时配置源。

规则 ID、证据字段、枚举值和 Spark 参数名保留英文原文，便于和 `AnalysisResult` JSON、CLI/UI 报告以及 Spark 配置对应。本文档以 **Apache Spark 3.5.1** 为主要基线。

## 阅读口径

SparkAdvisor 的规则分为两类：

- **运行时症状规则**：基于 event log / `TaskMetrics` / SparkAdvisor 派生指标识别现象，例如 Task 长尾、spill、GC、fetch wait、Stage 启动等待、attempt。
- **计划机会规则**：基于物理计划文本提示可能的优化方向，例如 broadcast join、sort/aggregate spill 可疑归因。

需要明确三点：

- `skewRatioWarn`、`spillRatioWarn`、`coreUtilLow` 等阈值是 SparkAdvisor 自定义 heuristics，不是 Spark 官方阈值。
- Event log 中的 Stage/Task 汇总指标不等同于 Spark SQL 算子级指标；当 Spark UI SQL tab 或 SQL execution detail 有 operator metric 时，应优先用 operator metric 做二次确认。
- AQE 分区调优需要特别核对 `spark.sql.adaptive.coalescePartitions.parallelismFirst`。Spark 3.2+ 默认值为 `true`，表示 AQE coalesce 会优先维持并行度，而不是严格按 `spark.sql.adaptive.advisoryPartitionSizeInBytes` 生成目标分区大小。若目标是让 AQE 更明显地尊重 target size，通常需要评估 `spark.sql.adaptive.coalescePartitions.parallelismFirst=false` 的影响。

## R1_DATA_SKEW：数据倾斜/长尾偏斜

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

这是**通用长尾偏斜症状**，说明少数 Task 显著慢于中位 Task，wall clock 可能被长尾限制。常见原因包括热点 key、单分区过大、远端 shuffle 抖动、坏节点、对象创建/GC、CPU-heavy UDF 等。

注意：Task duration skew 是症状，不等于 Spark 官方 AQE skew join 的触发条件。Spark 3.5.1 的 AQE skew join 主要处理 shuffled join 的分区大小偏斜，判定口径是 shuffle partition bytes 同时超过中位数倍数阈值和字节阈值；源码路径覆盖 `SortMergeJoinExec` 与 `ShuffledHashJoinExec`，并受 join type 与可 split side 限制。对 group-by、window、UDF 或非 join 热点，AQE skew join 不会自动兜底。

建议动作：

- 若物理计划是 `SortMergeJoin` / `ShuffledHashJoin`，且证据主要表现为 shuffle partition bytes skew，可优先核对 `spark.sql.adaptive.skewJoin.enabled`、`spark.sql.adaptive.skewJoin.skewedPartitionFactor`、`spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`。
- `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` 通常不应低于 `spark.sql.adaptive.advisoryPartitionSizeInBytes`，否则可能过度敏感，带来额外 split 与复制成本。
- 已知热点 join/group key 时，优先考虑 salting、预聚合、显式 repartition 或上游数据分布治理。
- 若长尾来自 UDF、GC、fetch wait 或失败重试，应联动 R6/R9/R10，而不是只调 skew join 参数。

注意事项：

倾斜场景下，单纯增加 Executor 或盲目调整 `spark.sql.shuffle.partitions` 通常不能消除最长 Task。应先区分“join 分区大小偏斜”与“通用 straggler 偏斜”。

## R2_EXCESSIVE_SPILL：过量 Spill

命中条件：

- Stage spill bytes 大于 0。
- 当前实现使用 `spillBytes / max(shuffleReadBytes, 1) >= spillRatioWarn` 作为 reduce-side spill 触发信号。

证据字段：

- `spillBytes`
- `shuffleReadBytes`
- `spillRatio`

诊断含义：

`memoryBytesSpilled` / `diskBytesSpilled` 是 Spark 官方 Task 指标，但 Stage 级 spill 不是单一根因。应区分两种口径：

- **Reduce-side spill 压力**：Stage 有明显 shuffle read，且 `spillBytes / shuffleReadBytes` 较高。常见原因是 reducer 分区过大、倾斜或单 Task 内存预算不足。
- **通用算子 spill 压力**：Stage 几乎没有 shuffle read 但发生 spill，可能来自 scan-side sort、hash aggregate、object aggregate、window、external sort 或 shuffle write 等路径。此时 `spillBytes / max(shuffleReadBytes, 1)` 会被放大，不能直接解读为“shuffle 太大”。

建议动作：

- reduce-side spill 场景下，降低单 reducer 数据量：AQE 场景评估更小的 `spark.sql.adaptive.advisoryPartitionSizeInBytes`，非 AQE 场景评估增加 `spark.sql.shuffle.partitions`。
- AQE 场景必须同时核对 `spark.sql.adaptive.coalescePartitions.parallelismFirst`；默认 `true` 时，单改 `advisoryPartitionSizeInBytes` 不一定明显改变最终分区大小。
- 通用算子 spill 场景下，优先减少 sort/aggregate/window 前的工作集，例如提前过滤、裁剪列、预聚合，或结合 R11 到 SQL tab 核对算子级 `spill size` / `peak memory`。
- 适当增加 `spark.executor.memory` / `memoryOverhead` 可提高内存余量，但不应替代倾斜、工作集或分区大小治理。

注意事项：

当 `shuffleReadBytes` 很小或为 0 时，`spillRatio` 只表示“存在 spill 且分母不足”，不能把比率本身当作 reducer spill 严重程度。

## R3_LOW_PARALLELISM：并行利用不足

命中条件：

- SQL 级 `coreUtilization < coreUtilLow`。
- `coreUtilization > 0`，避免缺失容量数据时误报。

证据字段：

- `coreUtilization`
- `criticalPathMs`
- `idealMs`

诊断含义：

`coreUtilization` 是 SparkAdvisor 基于 task time 与 core timeline 推导的硬指标，不是 Spark 官方原生 TaskMetrics。它说明 Executor slot 没有被充分利用，但不必然等于“分区数不足”。

常见原因包括：

- Stage 分区数少，不能填满可用 Core。
- AQE coalesce 过度，最终分区数过少。
- dynamic allocation 冷启动、资源池排队或 locality wait。
- DAG 层面存在先天串行依赖，关键路径限制并行度。
- 上游长尾 Task 使下游资源短时空闲。

建议动作：

- 先联动 R8、executor scaling 预测、Stage task 数与可用 slot 对比，判断低利用率来自“没有足够 runnable task”还是“资源/启动等待”。
- 若确认是分区过少，AQE 场景可评估降低 `spark.sql.adaptive.advisoryPartitionSizeInBytes` 或调高 `coalescePartitions.initialPartitionNum`。
- 若希望 AQE 明显改变最终分区数，需要核对 `spark.sql.adaptive.coalescePartitions.parallelismFirst`。
- 非 AQE 场景可评估增加 `spark.sql.shuffle.partitions` 或对输入 repartition。

注意事项：

不要把 R3 单独解读为“应该加 partitions”。如果 R1/R9/R10 命中，低利用率可能只是长尾、fetch wait 或重试造成的副作用。

## R4_OVER_PARALLELISM：过并行小 Task

命中条件：

- Stage `numTasks >= overParallelMinTasks`。
- Stage `medianTaskMs < smallTaskMedianMs`。

证据字段：

- `numTasks`
- `medianTaskMs`

诊断含义：

Task 很多但单个 Task 很短，调度、启动、反序列化等固定开销可能超过实际计算。常见原因是分区过多、小文件、过度 repartition 或写出端产生大量小输出分区。

建议动作：

- AQE coalesce 开启时，可评估调高 `spark.sql.adaptive.advisoryPartitionSizeInBytes`，但必须核对 `spark.sql.adaptive.coalescePartitions.parallelismFirst`。默认 `true` 时，Spark 可能优先保持并行度，调大 advisory size 不一定显著减少分区。
- 非 AQE 场景可降低 `spark.sql.shuffle.partitions`。
- `coalesce()` 更适合终端结果或写出前降低输出分区数，不应机械套用到所有上游 Stage；上游 Stage 若过早 coalesce，可能牺牲并行度并放大单 Task 数据量。
- 若同时命中 R5，应优先处理小文件或 scan partition 合并。

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

- 上游做文件合并、周期性 compaction，或写入时使用合理的 repartition/coalesce。
- 调整 `spark.sql.files.maxPartitionBytes` / `spark.sql.files.openCostInBytes`，让 Spark 每个 scan Task 合并更多小文件。
- Spark 3.5.0+ 可评估 `spark.sql.files.maxPartitionNum`，为文件扫描分区数量设置建议上限。
- 若该 SQL 以写出为终点，可评估 `REBALANCE` hint、写侧 repartition 或 compaction，降低后续重复读取成本。`REBALANCE` 依赖 AQE，目标是让输出分区大小更合理。

## R6_GC_PRESSURE：GC 压力

命中条件：

- Stage `gcRatio >= gcRatioWarn`。

证据字段：

- `gcRatio`

诊断含义：

`jvmGCTime` 是 Spark 官方 Task 指标，但 `gcRatio = jvmGCTime / taskTime` 仍是启发式。Spark 的 `executorRunTime` 包含 shuffle fetch 等等待时间，因此该比例不能替代 JVM profiling。

GC 高常见原因包括对象创建重的 UDF、非 codegen 路径、object-heavy aggregation、行宽膨胀、cache 行为、单 Task 工作集过大或 spill 伴随的内存压力。

建议动作：

- 第一优先级：检查对象创建重的 UDF、字符串/复杂对象处理、object aggregation、非 Tungsten/codegen 路径和 cache 使用。
- 第二优先级：降低单 Task 工作集，例如更多合理分区、更小 advisory size、提前过滤/裁剪列/预聚合。
- 第三优先级：结合容器资源评估 `spark.executor.memory`、`spark.executor.memoryOverhead`，以及 `spark.executor.extraJavaOptions` / `spark.executor.defaultJavaOptions` 中的 GC 参数。

注意事项：

大堆不一定降低 GC。对象 churn 问题上盲目放大 heap 可能使 pause 更长，应先确认 GC 压力来自容量不足还是对象分配路径。

## R7_BROADCAST_JOIN：Broadcast Join 机会

命中条件：

- 物理计划包含 `SortMergeJoin`。
- 物理计划不包含 `BroadcastHashJoin` 或 `BroadcastNestedLoopJoin`。

证据字段：

- `planHasSortMergeJoin`
- `planHasBroadcastJoin`

诊断含义：

这是物理计划启发式规则。Event log 通常只告诉我们最终执行计划和指标，不直接说明 planner 为什么没有选择 broadcast。该规则只提示“可能有机会”：如果某一侧足够小、join type 支持、统计信息可靠，并且没有更高优先级 hint 冲突，broadcast 可以避免一次 shuffle。

命中后的必查项：

- 小表侧实际大小来自静态 catalog stats 还是 AQE runtime stats，`sizeInBytes` 是否可信。
- join type 是否允许目标侧作为 build side。
- AQE 是否启用，以及是否配置 `spark.sql.adaptive.autoBroadcastJoinThreshold`。
- 是否存在 hint 冲突。Spark hint 优先级大致为 `BROADCAST` > `MERGE` > `SHUFFLE_HASH` > `SHUFFLE_REPLICATE_NL`。
- 是否存在 `spark.sql.broadcastTimeout`、driver/executor 内存或网络广播风险。

建议动作：

- 小表侧可放入内存且 join type 支持时，评估 `spark.sql.autoBroadcastJoinThreshold` 或 AQE 下的 `spark.sql.adaptive.autoBroadcastJoinThreshold`。
- 明确知道小表侧时，可添加 `broadcast()` hint。
- 若 stats 缺失或明显不可信，先补齐 `ANALYZE TABLE` / catalog stats，再谈阈值调优。

注意事项：

阈值调高不会保证 Spark 一定 broadcast。full outer join 等 join type、错误 stats、冲突 hint、broadcast timeout 或内存风险都可能阻止计划按预期变化。

## R8_STAGE_STARTUP_DELAY：Stage 启动/资源等待

兼容说明：运行时规则 ID 仍为 `R8_SCHEDULING_DELAY`，文档标题改为 `R8_STAGE_STARTUP_DELAY` 是为了准确表达口径。

命中条件：

- Stage wall clock 大于 0。
- `schedulingDelayMs / wallClockMs >= schedulingDelayRatioWarn`。

证据字段：

- `schedulingDelayMs`
- `stageWallMs`
- `delayRatio`

诊断含义：

`schedulingDelayMs` 是 SparkAdvisor 派生指标，不是 Spark 官方通用 TaskMetrics 字段。其定义为：

```text
schedulingDelayMs = firstTaskLaunchTime - stageSubmissionTime
```

它表示 Stage 从提交到首个 Task 启动之间的间隔，更准确的含义是 Stage startup / resource queue delay。常见原因包括 dynamic allocation 冷启动、cluster manager 资源排队、FAIR scheduler pool 争用、locality wait 或 SHS/event log 中可见的其它启动前等待。

建议动作：

- 对低延迟场景预热 Executor，例如评估 `spark.dynamicAllocation.minExecutors`、`spark.dynamicAllocation.initialExecutors` 与 `spark.dynamicAllocation.schedulerBacklogTimeout`。
- 检查 `spark.locality.wait` 及其分级参数是否造成过长 locality 等待。
- 固定资源队列下，结合队列报告判断是否存在资源池争用。

注意事项：

不要把该指标理解为 Spark 内核调度器本身“慢”。它只描述从 Stage submission 到首个 Task launch 的派生时间区间。

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

`shuffleReadMetrics.fetchWaitTime` 是 Spark 官方指标，表示 reducer Task 阻塞等待远端 shuffle block 的时间。该规则说明瓶颈更可能在远端 shuffle、网络、shuffle service、磁盘或 reducer 分区过大，而不是纯 CPU 计算。

建议动作：

- 在 Exchange 前减少 shuffle 数据量，例如提前过滤、裁剪列、预聚合。
- 检查 shuffle service、网络、磁盘、executor locality 与热点 executor。
- 如果 reducer 分区过大，增加 reducer 并行度或降低 AQE advisory partition size。
- 深入排查时可关注 `spark.reducer.maxSizeInFlight`、`spark.reducer.maxReqsInFlight`、`spark.reducer.maxBlocksInFlightPerAddress` 等远端 block fetch 相关参数。

注意事项：

R9 需要多 executor 或真实远端 shuffle 场景才有代表性。全本地读取或 local mode 下，该指标可能不足以反映生产网络瓶颈。

## R10_TASK_ATTEMPTS：Task 失败与 speculative attempt

兼容说明：运行时规则 ID 仍为 `R10_TASK_RETRY`。文档标题改为 `R10_TASK_ATTEMPTS`，是为了明确区分失败 attempt 与 speculative attempt。

命中条件：

- `failedTaskAttempts >= failedTaskAttemptsWarn`。
- 或 `extraTaskAttempts / numTasks >= extraTaskAttemptRatioWarn`。

证据字段：

- `failedTaskAttempts`
- `extraTaskAttempts`
- `extraTaskAttemptRatio`

诊断含义：

`failedTaskAttempts` 是失败信号，但 `extraTaskAttempts` 不一定是失败。若 `spark.speculation=true`，Spark 可能主动为慢 Task 启动 speculative attempt；这种额外 attempt 是 straggler mitigation，不应直接等同于失败重试。

建议把命中结果分成两类解读：

- **失败 attempt**：`failedTaskAttempts > 0`。重点排查 OOM、timeout、坏节点、容器/磁盘/网络异常、外部系统波动或数据异常。
- **Speculative/extra attempt**：`failedTaskAttempts = 0` 但 `extraTaskAttempts > 0`，尤其在 `spark.speculation=true` 时。重点联动 R1/R6/R8/R9 找 straggler 根因，不应先关闭 speculation。

建议动作：

- 对失败 attempt，检查失败 Task 日志、executor/container 健康状态、OOM/timeout/坏节点和外部依赖。
- 若失败与大分区、内存或超时相关，降低单 Task 输入或 shuffle 数据量。
- 对 speculative attempt，排查倾斜、fetch wait、GC、坏节点或磁盘慢；只有确认 speculation 本身造成资源副作用时，才考虑调整 speculation 参数。

注意事项：

当前 event log 汇总层若无法可靠区分 speculative attempt 原因，报告应降低归因置信度。不要把 `extraTaskAttempts` 简化为“失败重试”。

## R11_SORT_AGG_SPILL：Sort/Aggregate Spill 可疑归因

命中条件：

- 物理计划包含 `Sort`、`HashAggregate`、`ObjectHashAggregate` 或 `SortAggregate`。
- SQL 的 Stage 总 spill 大于 0。

证据字段：

- `planHasSort`
- `planHasAggregate`
- `totalSpillBytes`

诊断含义：

这是启发式关联规则，只提示 sort/aggregate 可能是 spill 的热点位置。Event log 的 spill 是 Stage/Task 粒度，不直接标注具体算子；有 `Sort`/`Aggregate` 不代表 spill 一定来自这些算子。真实 spill 也可能来自 sort-merge join、window、shuffle write 或其它下游 operator。

建议动作：

- 优先到 Spark UI SQL tab / SQL execution detail 核对 operator-level spill metric、peak memory、fallback 等指标；若存在算子级 metric，应以 operator metric 为准。
- 若确认 sort/aggregate 是主要 spill 点，在 sort/aggregate 前减少工作集，例如提前过滤、裁剪列、预聚合。
- 对发生 spill 的 Stage 降低单分区数据量，或增加内存余量。

注意事项：

不要把 R11 当作确定归因。它的价值是给二次核对提供方向，而不是替代 SQL tab 的算子级指标。

## 待补规则方向

当前 R1-R11 已覆盖主要运行时症状与部分物理计划机会，但仍有一些 Spark SQL 优化前提值得后续补入规则库：

- **统计信息与 CBO 置信度**：当 join 相关规则命中但缺少可靠 `sizeInBytes` / catalog stats / runtime stats 时，应先建议补齐 `ANALYZE TABLE` 或 catalog stats，再调 broadcast 阈值。
- **Shuffled Hash Join 机会**：Spark 3.2+ AQE 可在 post-shuffle partitions 足够小时把 SMJ 转成 SHJ，相关参数包括 `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold`。
- **动态分区裁剪与运行时过滤**：分区表 join + 选择性过滤场景下，若 DPP/runtime filter 未生效，可能本应在更上游减少扫描与 shuffle。
- **写侧文件尺寸与 REBALANCE**：R5 覆盖读侧小文件，但写出端若产生大量小文件或偏斜大文件，应单独提示 `REBALANCE`、repartition 或 compaction。

