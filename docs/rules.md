# SparkAdvisor 规则目录

本文档是规则说明书，供开发、运维和调优排查时查阅。运行时规则仍以 Java `RuleEngine` 为准；本文档不作为运行时配置源。

规则 ID、证据字段、枚举值和 Spark 参数名保留英文原文，便于和 `AnalysisResult` JSON、CLI/UI 报告以及 Spark 配置对应。本文档以 **Apache Spark 3.5.1** 为主要基线。

## 阅读口径

SparkAdvisor 的规则分为两层：

- **单 SQL 规则（R1-R11）**：面向一条 SQL 的 stage/task/plan 证据，产出 `AnalysisResult.findings`，用于单条 SQL 下钻诊断。
- **队列级规则（Q1-Q11）**：面向一个长驻 Spark Application / 查询队列的一整轮 SQL 聚合证据，产出 `QueueAnalysisResult.globalRecommendations`，用于判断固定资源池的资源使用、查询频率、耗时分布、争用、全局参数失配和机制类优化机会。

单 SQL 规则按证据类型又分为两类：

- **运行时症状规则**：基于 event log / `TaskMetrics` / SparkAdvisor 派生指标识别现象，例如 Task 长尾、spill、GC、fetch wait、Stage 启动等待、attempt。
- **计划机会规则**：基于物理计划文本提示可能的优化方向，例如 broadcast join、sort/aggregate spill 可疑归因。

需要明确以下几点：

- `skewRatioWarn`、`spillRatioWarn`、`coreUtilLow` 等阈值是 SparkAdvisor 自定义 heuristics，不是 Spark 官方阈值。
- Event log 中的 Stage/Task 汇总指标不等同于 Spark SQL 算子级指标；当 Spark UI SQL tab 或 SQL execution detail 有 operator metric 时，应优先用 operator metric 做二次确认。
- AQE 分区调优需要特别核对 `spark.sql.adaptive.coalescePartitions.parallelismFirst`。Spark 3.2+ 默认值为 `true`，表示 AQE coalesce 会优先维持并行度，而不是严格按 `spark.sql.adaptive.advisoryPartitionSizeInBytes` 生成目标分区大小。若目标是让 AQE 更明显地尊重 target size，通常需要评估 `spark.sql.adaptive.coalescePartitions.parallelismFirst=false` 的影响。
- 队列级结论不是把多个单 SQL finding 简单相加。Q 规则必须同时看查询频率、耗时分位、资源池占用、CPU/fetch/GC/attempt 效率、抽样覆盖率和争用归因；当深分析样本覆盖率不足时，置信度必须下调。

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

## 队列级规则总览（Q1-Q11）

队列级规则由 `sparkadvisor-monitor` 的 `QueueRuleEngine` 生成，输入是 `QueueAnalysisResult`，不是单条 SQL 的 `AnalysisResult`。它回答的问题是：

- 这个固定 executor/core 池在一整轮运行中是否长期跑满？
- 查询频率、耗时 P50/P95/P99 和资源效率在什么时间段恶化？
- 慢查询到底是自身计划/数据问题，还是被其它查询挤占资源，还是 slot 被 fetch/GC/失败 attempt 等低效等待占住？
- 同一个全局 Spark 参数是否同时伤害大查询和小查询？
- 哪些调优动作适合做成队列级默认参数，哪些只适合单 SQL 或单模板下钻处理？

### 队列级结果契约

Q 规则只依赖 `QueueAnalysisResult`，核心字段如下：

| 字段 | 含义 | 用途 |
| --- | --- | --- |
| `summary.totalQueries/completedQueries/runningQueries/failedQueries` | 一轮 app 内 SQL 总量、已完成、运行中与失败数量 | 查询频率、失败率、报告完整性 |
| `summary.windowStart/windowEnd` | 分析窗口 | 按小时/分钟分桶，识别高峰期 |
| `summary.fixedExecutorCores` | 固定资源池容量 | slot 占用率和容量判断分母 |
| `timeline[].queryCount/p50Ms/p95Ms/p99Ms` | 时间桶内查询数和耗时分位 | 查询频率、延迟趋势、峰谷对比 |
| `timeline[].avgUtilization/cpuEfficiency/fetchWaitRatio/gcRatio/failedAttemptRatio/speculativeAttemptRatio` | 时间桶资源效率 | 判断 slot 被占满后 CPU 是否真忙 |
| `utilization.avgUtilization/peakUtilization` | 全局平均/峰值资源池占用 | 容量、并发、空闲判断 |
| `resources.totalSpillBytes/avgCpuEfficiency/avgFetchWaitRatio/avgGcRatio/p95MaxGcRatio` | 全局资源症状 | spill、fetch、GC、CPU 效率 |
| `contention.contentionLimitedPct/inefficientBusyPct` | 争用受限与低效忙碌查询占比 | 区分资源不足和阻塞低效 |
| `contention.hotspots/starvationWindows/topResourceHogs` | 高占用窗口、饥饿窗口、资源大户 | 分池、限流、隔离建议 |
| `bottlenecks[].ruleId/affectedQueries/affectedPct/sampleCoveragePct/scope` | 重复单 SQL 瓶颈，`scope` 可为 `FULL_QUEUE_LIGHT`、`DEEP_SAMPLE` 或 `FULL_QUEUE_LIGHT+DEEP_SAMPLE` | 把 R 规则升级为队列级证据；能用轻特征判断的资源类规则按全量 SQL 分母统计 |
| `topSlowQueries/sampledQueries` | 最慢 SQL 与分层抽样 SQL | 下钻、校验抽样代表性 |
| `meta.lightAnalyzedQueries/deepAnalyzedQueries/deepCoveragePct/samplingStrategy/degradedReason` | 分析覆盖率与降级原因 | 置信度与 caveat |

### 队列级判断口径

固定查询队列至少需要同时看三类指标：

1. **查询频率与时长**：`queryCount`、P50/P95/P99、失败/运行中 SQL 数。高频中等慢模板可能比单条最慢 SQL 更值得全局治理。
2. **资源使用情况**：`slotOccupancy/avgUtilization` 说明资源池是否被占满；`cpuEfficiency` 说明占住 slot 后 CPU 是否真在执行；`fetchWaitRatio/gcRatio/attemptRatio` 说明低效原因。
3. **瓶颈覆盖面**：`bottlenecks` 只来自深分析样本，必须结合 `sampleCoveragePct/deepCoveragePct`。低覆盖率时，不应把 top-N 慢查询的 finding 推广为全队列事实。

队列级规则默认阈值来自 `QueueRuleThresholds.defaults()`：

| 阈值 | 默认值 | 解释 |
| --- | --- | --- |
| `minAnalyzedQueries` | 5 | 重复瓶颈至少命中多少个深分析 SQL 才算常见 |
| `commonBottleneckPct` | 0.30 | 或者命中深分析样本占比达到多少算常见 |
| `mixedPartitionPct` | 0.15 | 大查询欠并行与小查询过并行并存的最低占比 |
| `highUtilization` | 0.85 | 平均资源池占用高 |
| `lowUtilization` | 0.35 | 平均资源池占用低 |
| `contentionLimitedPct` | 0.25 | 争用/低效受限查询占比较高 |
| `lowCpuEfficiency` | 0.35 | CPU 效率低 |

这些阈值是 SparkAdvisor 的启发式，不是 Spark 官方阈值。生产环境应结合队列 SLA、查询类型和资源规模校准。

## Q1_COMMON_SPILL_PRESSURE：队列普遍 Spill 压力

命中条件：

- 深分析样本中 `R2_EXCESSIVE_SPILL` 命中数达到 `minAnalyzedQueries`。
- 或 `R2_EXCESSIVE_SPILL.affectedPct >= commonBottleneckPct`。

证据字段：

- `bottlenecks[ruleId=R2_EXCESSIVE_SPILL].affectedQueries`
- `bottlenecks[ruleId=R2_EXCESSIVE_SPILL].affectedPct`
- `bottlenecks[ruleId=R2_EXCESSIVE_SPILL].sampleCoveragePct`
- `resources.totalSpillBytes`

诊断含义：

队列里多条 SQL 反复 spill，说明 spill 不是孤立查询异常，可能来自 reducer 分区过大、倾斜、sort/aggregate 工作集过大、内存预算不足或小文件/过分区导致的调度放大。队列级建议必须先区分 spill 形态，不应直接把所有 spill 解释为“executor memory 太小”。

建议动作：

- 先下钻代表性 SQL，区分 reduce-side spill 与 sort/aggregate/window/operator spill。
- reduce-side spill 常见于大 reducer 分区或倾斜：AQE 场景评估 `spark.sql.adaptive.advisoryPartitionSizeInBytes`、`spark.sql.adaptive.coalescePartitions.initialPartitionNum` 与 `spark.sql.adaptive.coalescePartitions.parallelismFirst`；非 AQE 场景评估 `spark.sql.shuffle.partitions`。
- 如果 spill 主要集中在少数热点模板，优先 SQL 改写、预聚合、过滤裁剪或热点 key 治理，不要把全局内存参数作为第一动作。
- 只有确认多条 SQL 都受单 Task 内存预算限制时，再评估 `spark.executor.memory`；`spark.executor.memoryOverhead` 主要用于 PySpark/native/off-heap/container overhead，不是 JVM SQL spill 的默认修复。

注意事项：

低深分析覆盖率时，该规则至多说明“最慢/抽样 SQL 中 spill 常见”，不能自动代表全队列。

## Q2_COMMON_LONG_TAIL_SKEW：队列普遍长尾/倾斜

命中条件：

- 深分析样本中 `R1_DATA_SKEW` 命中数达到 `minAnalyzedQueries`。
- 或 `R1_DATA_SKEW.affectedPct >= commonBottleneckPct`。

证据字段：

- `bottlenecks[ruleId=R1_DATA_SKEW].affectedQueries`
- `bottlenecks[ruleId=R1_DATA_SKEW].affectedPct`
- `topSlowQueries[].dominantBottleneck`

诊断含义：

多条 SQL 被长尾 task 限制，队列层面可能存在共享数据分布问题、热点 key 模板、join/group-by 模式重复、AQE skew join 未生效或只覆盖部分场景。此时加 executor/core 或盲目改 `spark.sql.shuffle.partitions` 通常不能消除最长 task。

建议动作：

- 核对 `spark.sql.adaptive.skewJoin.enabled`，并结合 `spark.sql.adaptive.skewJoin.skewedPartitionFactor`、`spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` 与 `spark.sql.adaptive.advisoryPartitionSizeInBytes` 判断是否过于迟钝或过于敏感。
- 对重复模板下钻，确认是否是相同 join/group key 热点；已知热点时优先 salting、预聚合、拆分异常 key 或上游数据分布治理。
- 如果长尾伴随 fetch wait、GC 或 failed/speculative attempt，应联动 Q5/Q8，而不是只处理 skew 参数。

注意事项：

AQE skew join 主要处理 shuffled join 的分区大小偏斜；group-by、window、UDF、外部服务调用或坏节点造成的长尾不一定能被 skew join 参数修复。

## Q3_STATIC_PARTITIONS_CONFLICT：静态分区策略失配

命中条件：

- 深分析样本同时存在 `R3_LOW_PARALLELISM` 与 `R4_OVER_PARALLELISM`。
- 两类瓶颈各自 `affectedPct >= mixedPartitionPct`。

证据字段：

- `bottlenecks[ruleId=R3_LOW_PARALLELISM].affectedPct`
- `bottlenecks[ruleId=R4_OVER_PARALLELISM].affectedPct`
- `sampledQueries[].deepAnalyzed`

诊断含义：

同一个固定队列中，大查询嫌分区太少、小查询嫌 task 太碎，说明一个静态 `spark.sql.shuffle.partitions` 或一套固定 repartition 习惯正在双向伤害 workload。队列级调优的重点应从“找一个完美固定值”转为“让分区策略自适应查询大小”。

建议动作：

- 开启并核对 AQE：`spark.sql.adaptive.enabled=true`、`spark.sql.adaptive.coalescePartitions.enabled=true`。
- 评估 `spark.sql.adaptive.advisoryPartitionSizeInBytes` 与 `spark.sql.adaptive.coalescePartitions.initialPartitionNum`，让大查询保留足够上限、小查询可被 coalesce。
- 对繁忙共享队列，评估 `spark.sql.adaptive.coalescePartitions.parallelismFirst=false` 是否更符合目标分区大小。该参数可能降低并行度，必须结合 Q4/Q6 判断资源池是否长期跑满。
- 避免仅用一个固定 `spark.sql.shuffle.partitions` 解决所有大小查询；必要时按 SQL 模板/业务入口分层设置。

注意事项：

如果低并行来自资源等待、长尾或 DAG 串行依赖，调分区不能直接解决。必须联动 R1/R8/Q4/Q5。

## Q4_CAPACITY_OR_CONCURRENCY_LIMITED：容量或并发受限

命中条件：

- `utilization.avgUtilization >= highUtilization`。
- `contention.contentionLimitedPct >= contentionLimitedPct`。
- `resources.avgCpuEfficiency >= lowCpuEfficiency`。

证据字段：

- `utilization.avgUtilization`
- `utilization.peakUtilization`
- `contention.contentionLimitedPct`
- `contention.hotspots`
- `contention.topResourceHogs`
- `resources.avgCpuEfficiency`

诊断含义：

固定 executor 池长期被占满，CPU 效率不低，同时不少慢查询在生命周期内拿到的 core 份额偏低。此时问题更像“共享资源池被并发挤占”，不是单条 SQL 的计划问题，也不是 fetch/GC 等低效等待主导。

建议动作：

- 固定队列优先考虑并发控制：限制同时运行大查询数、隔离资源大户、对长查询/短查询拆入口。
- FAIR scheduler 场景评估 pool、`minShare`、`weight`、业务优先级与长查询隔离；FIFO 场景关注 head-of-line blocking。
- 如果资源池全天高占用且 CPU 效率正常，再把增加 executor/core 作为容量规划选项，而不是单条 SQL 调优建议。
- 报告中应展示 `topResourceHogs` 和 `hotspots`，说明是谁在什么时候占用资源。

注意事项：

Event log 不直接记录 scheduler queue wait。争用受限是基于 task interval 的推断；多 pool、dynamic allocation、`spark.task.cpus > 1` 或外部系统等待较多时应降低置信度。

## Q5_BLOCKED_OR_INEFFICIENT_BUSY：Slot 忙但低效

命中条件：

- `utilization.avgUtilization >= highUtilization`。
- 且满足以下任一项：
  - `resources.avgCpuEfficiency < lowCpuEfficiency`。
  - `contention.inefficientBusyPct >= contentionLimitedPct`。

证据字段：

- `utilization.avgUtilization`
- `resources.avgCpuEfficiency`
- `resources.avgFetchWaitRatio`
- `resources.avgGcRatio`
- `resources.failedAttemptRatio`
- `resources.speculativeAttemptRatio`
- `contention.inefficientBusyPct`

诊断含义：

Executor slot 看起来很忙，但 CPU 并不高效执行，可能被 shuffle fetch、GC、失败重试、speculative attempt、外部 UDF/服务调用或磁盘/网络阻塞占住。此类场景下盲目加资源可能扩大等待面，不能解决根因。

建议动作：

- `avgFetchWaitRatio` 高：排查 shuffle service、网络、磁盘、热点 executor、reducer 分区大小；必要时调整 `spark.reducer.maxSizeInFlight`、`spark.reducer.maxReqsInFlight`、`spark.reducer.maxBlocksInFlightPerAddress`，但应结合平台指标验证。
- `avgGcRatio/p95MaxGcRatio` 高：优先查对象重 UDF、cache 布局、行宽、非 codegen/object aggregation，再考虑 `spark.executor.memory`、GC 参数和单 Task 数据量。
- failed/speculative attempt 高：先查失败原因、坏节点、OOM/timeout、数据异常或 straggler；不要把 extra attempt 简化为失败重试。
- 若外部服务/UDF 阻塞明显，应从外部依赖、连接池、超时和降级策略入手，而不是 Spark 分区参数。

注意事项：

需要结合集群 CPU、网络、磁盘和 executor 日志验证。Event log 的 `executorCpuTime`、fetch wait、GC 是强证据，但不是完整系统画像。

## Q6_IDLE_BUT_SLOW：资源池空闲但查询仍慢

命中条件：

- `utilization.avgUtilization <= lowUtilization`。
- `summary.completedQueries > 0`。

证据字段：

- `utilization.avgUtilization`
- `timeline[].queryCount`
- `timeline[].p95Ms/p99Ms`
- `bottlenecks`

诊断含义：

队列没有持续跑满，增加 executor/core 通常不是第一优先级。慢可能来自单 SQL 自身的倾斜、spill、scan 小文件、计划问题、上游/外部系统等待、低查询频率造成的冷启动或队列本身并发不足。

建议动作：

- 优先下钻慢模板的 R 规则，处理 SQL/数据布局/文件/计划问题。
- 如果查询频率低且存在 Stage startup delay，评估 `spark.dynamicAllocation.minExecutors`、`initialExecutors`、`schedulerBacklogTimeout` 或预热策略；固定 executor 队列则检查是否资源配置过大。
- 对低频批量查询，可能需要业务侧合并小查询或调度窗口，而不是扩大 Spark 资源。

注意事项：

如果 task interval 采集缺失，utilization 可能被低估。应检查 `meta.degradedReason`。

## Q7_COMMON_SMALL_FILES：队列普遍小文件

命中条件：

- 深分析样本中 `R5_SMALL_FILES` 命中数达到 `minAnalyzedQueries`。
- 或 `R5_SMALL_FILES.affectedPct >= commonBottleneckPct`。

证据字段：

- `bottlenecks[ruleId=R5_SMALL_FILES].affectedQueries`
- `bottlenecks[ruleId=R5_SMALL_FILES].affectedPct`

诊断含义：

多个查询反复被小文件 scan task 放大，说明这是上游数据布局问题，不是单次 SQL 参数问题。小文件会重复消耗 listing、open、task scheduling、metadata 和 scan overhead。

建议动作：

- 优先做上游 compaction、合理写出分区、表维护或数据生命周期治理。
- 评估 `spark.sql.files.maxPartitionBytes`、`spark.sql.files.openCostInBytes`，让每个 scan task 合并更多文件。
- Spark 3.5 可评估 `spark.sql.files.maxPartitionNum`，为 scan 分区数量设建议上限。
- 写侧场景评估 `REBALANCE` hint、写前 repartition/coalesce 或表级 compaction，避免继续制造小文件。

注意事项：

文件参数是缓解，不是根治。根治通常在上游写入和表维护。

## Q8_COMMON_GC_OBJECT_CHURN：队列普遍 GC / 对象分配压力

命中条件：

- 深分析样本中 `R6_GC_PRESSURE` 命中数达到 `minAnalyzedQueries`。
- 或 `R6_GC_PRESSURE.affectedPct >= commonBottleneckPct`。

证据字段：

- `bottlenecks[ruleId=R6_GC_PRESSURE].affectedQueries`
- `resources.avgGcRatio`
- `resources.p95MaxGcRatio`
- `timeline[].gcRatio`

诊断含义：

GC 压力跨多条 SQL 重复出现，可能是队列级代码路径或数据形态问题，例如对象重 UDF、字符串/复杂类型处理、cache 行宽、object aggregation、非 codegen/vectorization fallback、单 task 工作集过大。

建议动作：

- 查重复模板是否使用相同 UDF、复杂对象处理或 cache 表。
- 优先减少对象创建和单 task 工作集：过滤裁剪、预聚合、合理分区、避免不必要 cache。
- 再评估 `spark.executor.memory`、GC 参数、`spark.memory.fraction` 等 JVM 调优；调大 heap 可能降低频率但拉长 pause。

注意事项：

GC 比例来自 task metrics，不替代 JVM GC log/profiling。队列级建议应作为排查方向，不应直接给出单一 heap 值。

## Q9_FAIRNESS_OR_STARVATION：公平性/饥饿

命中条件：

- `contention.starvationWindows` 非空。
- 典型推断口径是查询处于高占用窗口，且自身获得的 core 份额长期偏低。

证据字段：

- `contention.starvationWindows`
- `contention.contentionLimitedPct`
- `contention.topResourceHogs`
- `topSlowQueries[].contentionLimited`
- `topSlowQueries[].ownCoreMs`

诊断含义：

部分查询并非自身算得慢，而是在资源池繁忙时长期拿不到足够 slot。短查询被长查询压住时，会表现为 P95/P99 恶化，但单 SQL 下钻可能看不出明显 R1/R2/R6。

建议动作：

- FAIR scheduler：按业务区分 pool，评估 `minShare`、`weight`、优先级和大查询隔离。
- FIFO 或单 pool：评估大查询并发限制、短查询单独入口、Admission control 或业务侧限流。
- 对 `topResourceHogs` 中的资源大户做模板治理，避免少数查询长期占满固定池。

注意事项：

Event log 对 pool/share 信息有限，公平性归因必须结合 scheduler 配置、业务入口和 Spark UI Jobs/SQL 时间线验证。

## Q10_STATS_CBO_OR_JOIN_STRATEGY：统计信息/CBO/Join 策略缺口

命中条件：

- 深分析样本中 `R7_BROADCAST_JOIN` 命中数达到 `minAnalyzedQueries`。
- 或 `R7_BROADCAST_JOIN.affectedPct >= commonBottleneckPct`。

证据字段：

- `bottlenecks[ruleId=R7_BROADCAST_JOIN].affectedQueries`
- 代表性 SQL 的物理计划文本
- `sampledQueries[].templateHash`

诊断含义：

多个查询反复出现可疑的 sort-merge join / 未 broadcast 机会，队列层面可能缺少可靠 catalog stats、CBO 输入、AQE runtime stats，或者存在 join hint、join type、broadcast timeout/内存限制。

建议动作：

- 先补统计信息：`ANALYZE TABLE`、列统计、表大小和分区统计；用 `EXPLAIN COST` 或 Spark UI runtime stats 校验。
- 核对 `spark.sql.cbo.enabled`、`spark.sql.statistics.size.autoUpdate.enabled` 等统计链路是否符合当前数据平台策略。
- 小表侧稳定且可放入内存时，再评估 `spark.sql.autoBroadcastJoinThreshold`、`spark.sql.adaptive.autoBroadcastJoinThreshold` 或 `broadcast()` hint。
- 对重复模板建立 join 策略白名单/下钻清单，而不是全局盲目调高 broadcast 阈值。

注意事项：

广播阈值调高可能带来 driver/executor 内存压力和 broadcast timeout。必须验证 build side 合法性与实际大小。

## Q11_QUEUE_EXECUTION_MECHANISM_GAPS：队列级执行机制缺口

命中条件：

- 存在 `R7_BROADCAST_JOIN`、`R9_SHUFFLE_FETCH_WAIT` 或 `R11_SORT_AGG_SPILL` 等机制类 cluster。
- 当前实现作为低置信度机制检查清单输出。

证据字段：

- `bottlenecks[ruleId=R7_BROADCAST_JOIN]`
- `bottlenecks[ruleId=R9_SHUFFLE_FETCH_WAIT]`
- `bottlenecks[ruleId=R11_SORT_AGG_SPILL]`
- `sampledQueries[].templateHash`

诊断含义：

队列反复出现 join strategy、shuffle fetch、sort/aggregate spill 等机制类症状，说明问题不一定能由一个容量或分区参数解决，可能需要从 Spark SQL 机制开关、表统计、数据布局、pushdown/vectorization、DPP/runtime filter、AQE join conversion 等路径系统排查。

建议动作：

- 对重复慢模板核对最终 AQE plan、SQL tab operator metrics、spill/fetch/peak memory、broadcast/runtime stats。
- 检查 `spark.sql.optimizer.dynamicPartitionPruning.enabled`、runtime filter、数据源 pushdown、Parquet/ORC vectorization、catalog stats 和 AQE join conversion。
- 如果机制缺口集中在少数模板，优先模板治理；如果跨模板普遍出现，再考虑队列默认参数。

注意事项：

Q11 是机制 checklist，不是确定根因。置信度通常为 `LOW`，除非有 operator metric 或外部系统指标补强。

## 队列级调参选项矩阵

| 观察到的队列证据 | 优先调优方向 | 可评估参数/机制 | 不建议的误用 |
| --- | --- | --- | --- |
| 高 `queryCount`、P95/P99 在高峰恶化、`avgUtilization` 高、`cpuEfficiency` 正常 | 并发治理与资源隔离 | FAIR pool `minShare/weight`、Admission control、分离短/长查询入口、容量规划 executor/core | 只调单 SQL 分区数 |
| `avgUtilization` 高但 `avgCpuEfficiency` 低 | 找阻塞来源 | shuffle fetch 参数、GC/对象分配、失败重试、外部 UDF/服务调用、平台网络/磁盘 | 盲目扩容 |
| 大查询 `R3_LOW_PARALLELISM` 与小查询 `R4_OVER_PARALLELISM` 并存 | 自适应分区策略 | AQE coalesce、`advisoryPartitionSizeInBytes`、`initialPartitionNum`、`parallelismFirst=false`、模板分层配置 | 找一个全局固定 `shuffle.partitions` |
| 多 SQL spill | 降低单 task 工作集与确认 spill 类型 | AQE advisory size、shuffle partitions、预聚合、过滤裁剪、executor memory | 直接加 `memoryOverhead` |
| 多 SQL 小文件 | 数据布局治理 | compaction、写侧 repartition/REBALANCE、`files.maxPartitionBytes/openCostInBytes/maxPartitionNum` | 只调 shuffle partitions |
| 多 SQL join 策略可疑 | stats/CBO/AQE join 治理 | `ANALYZE TABLE`、CBO、auto/adaptive broadcast threshold、broadcast hint、AQE join conversion | 全局大幅调高 broadcast 阈值 |
| 多 SQL fetch wait | Shuffle/网络/分区治理 | 减少 shuffle 数据量、reducer 分区大小、shuffle service、`spark.reducer.*` fetch 参数 | 只加 executor |
| 多 SQL GC | 代码路径和内存治理 | UDF/object churn、cache 布局、过滤裁剪、executor memory/GC 参数 | 只加 heap 且不改对象分配 |

## 待补规则方向

当前 R1-R11 已覆盖主要单 SQL 运行时症状与部分物理计划机会；Q1-Q11 覆盖了队列级资源利用、争用、频率/时长趋势和全局参数建议。但仍有一些 Spark SQL 优化前提值得后续补入规则库：

- **统计信息与 CBO 置信度**：当 join 相关规则命中但缺少可靠 `sizeInBytes` / catalog stats / runtime stats 时，应先建议补齐 `ANALYZE TABLE` 或 catalog stats，再调 broadcast 阈值。
- **Shuffled Hash Join 机会**：Spark 3.2+ AQE 可在 post-shuffle partitions 足够小时把 SMJ 转成 SHJ，相关参数包括 `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold`。
- **动态分区裁剪与运行时过滤**：分区表 join + 选择性过滤场景下，若 DPP/runtime filter 未生效，可能本应在更上游减少扫描与 shuffle。
- **写侧文件尺寸与 REBALANCE**：R5 覆盖读侧小文件，但写出端若产生大量小文件或偏斜大文件，应单独提示 `REBALANCE`、repartition 或 compaction。
- **队列级模板频率与总成本**：高频中等慢模板可能比单条最慢 SQL 更消耗总资源，应在 Q 规则中显式纳入 `templateHash` 的出现次数、总耗时和总 core-ms。
- **调度池证据增强**：当前 Q9 主要基于 task interval 推断饥饿；若后续能稳定抽取 FAIR pool、minShare、weight 与 pool 切换信息，应把公平性规则从 MEDIUM/LOW 置信度提升为更可操作的配置建议。
- **写入端队列治理**：队列级小文件治理目前主要沿用 R5 读侧证据；后续应补充写出文件数、文件大小分布和 downstream read amplification 的闭环规则。
