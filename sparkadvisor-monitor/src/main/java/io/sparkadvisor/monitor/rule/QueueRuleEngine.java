package io.sparkadvisor.monitor.rule;

import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.predict.Confidence;
import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Queue-level rules that turn repeated cross-SQL evidence into global recommendations.
 */
public final class QueueRuleEngine {

    private final QueueRuleThresholds thresholds;

    public QueueRuleEngine() {
        this(QueueRuleThresholds.defaults());
    }

    public QueueRuleEngine(QueueRuleThresholds thresholds) {
        this.thresholds = thresholds;
    }

    public List<QueueAnalysisResult.QueueRecommendation> recommend(QueueAnalysisResult result) {
        List<QueueAnalysisResult.QueueRecommendation> recs = new ArrayList<>();
        int completed = result.summary().completedQueries();

        cluster(result, "R2_EXCESSIVE_SPILL").ifPresent(c -> {
            if (common(c, result)) {
                recs.add(rec("Q1_COMMON_SPILL_PRESSURE",
                        Recommendation.conf("Reduce skew/oversized reducer partitions first; increase spark.executor.memory only after partition shape is sane",
                                "Recurring spill can come from reduce-side partition size, operator working sets, or skew; memoryOverhead is not the normal JVM SQL spill fix.",
                                "Expected to help queries represented by the repeated R2 findings."),
                        evidence(c),
                        Confidence.MEDIUM,
                        coverage(c, completed),
                        caveat(result, c)));
            }
        });

        cluster(result, "R1_DATA_SKEW").ifPresent(c -> {
            if (common(c, result)) {
                recs.add(rec("Q2_COMMON_LONG_TAIL_SKEW",
                        Recommendation.conf("Audit AQE skew-join settings and skewed join keys",
                                "Repeated skew findings are usually key-distribution issues; changing only shuffle.partitions is unlikely to fix them.",
                                "Expected to help skew-limited slow queries."),
                        evidence(c),
                        Confidence.HIGH,
                        coverage(c, completed),
                        caveat(result, c)));
            }
        });

        Optional<QueueAnalysisResult.BottleneckCluster> lowParallel =
                cluster(result, "R3_LOW_PARALLELISM");
        Optional<QueueAnalysisResult.BottleneckCluster> overParallel =
                cluster(result, "R4_OVER_PARALLELISM");
        if (lowParallel.isPresent() && overParallel.isPresent()
                && lowParallel.get().affectedPct() >= thresholds.mixedPartitionPct()
                && overParallel.get().affectedPct() >= thresholds.mixedPartitionPct()) {
            recs.add(rec("Q3_STATIC_PARTITIONS_CONFLICT",
                    Recommendation.conf("Use AQE coalescing/advisoryPartitionSizeInBytes and consider parallelismFirst=false for this busy shared queue",
                            "The queue contains both under-partitioned large queries and over-partitioned small queries, so one static shuffle.partitions value is fighting itself.",
                            "Expected to improve mixed workloads without penalizing small queries as much."),
                    evidence(lowParallel.get()) + "; " + evidence(overParallel.get()),
                    Confidence.HIGH,
                    "Covers both affected groups in the queue evidence.",
                    caveat(result, lowParallel.get(), overParallel.get())));
        }

        double avgUtil = result.utilization().avgUtilization();
        if (avgUtil >= thresholds.highUtilization()
                && result.contention().contentionLimitedPct() >= thresholds.contentionLimitedPct()
                && result.resources().avgCpuEfficiency() >= thresholds.lowCpuEfficiency()) {
            recs.add(rec("Q4_CAPACITY_OR_CONCURRENCY_LIMITED",
                    Recommendation.conf("Limit/isolate resource-hog queries or tune FAIR pool weight/minShare before treating this as pure capacity expansion",
                            "The shared executor pool is saturated, CPU efficiency is not obviously low, and many slow queries appear contention-limited.",
                            "Expected to reduce latency for contention-limited queries."),
                    "avgUtilization=" + pct(avgUtil)
                            + ", cpuEfficiency=" + pct(result.resources().avgCpuEfficiency())
                            + ", contentionLimitedPct=" + pct(result.contention().contentionLimitedPct()),
                    Confidence.MEDIUM,
                    pct(result.contention().contentionLimitedPct()) + " of completed queries were classified as contention-limited.",
                    "Event log does not directly record scheduler queue wait; FAIR/multi-pool deployments need pool evidence."));
        }

        if (avgUtil >= thresholds.highUtilization()
                && (result.resources().avgCpuEfficiency() < thresholds.lowCpuEfficiency()
                || result.contention().inefficientBusyPct() >= thresholds.contentionLimitedPct())) {
            recs.add(rec("Q5_BLOCKED_OR_INEFFICIENT_BUSY",
                    Recommendation.conf("Do not add resources blindly; split the dominant blocked signal into shuffle fetch, GC/object churn, or failed/speculative attempts",
                            "Slots are busy but CPU efficiency is low or many queries are inefficient-busy, so the bottleneck may be network, GC, retries, or external calls.",
                            "Expected to avoid misclassifying blocked tasks as executor shortage."),
                    "avgUtilization=" + pct(avgUtil)
                            + ", cpuEfficiency=" + pct(result.resources().avgCpuEfficiency())
                            + ", fetchWaitRatio=" + pct(result.resources().avgFetchWaitRatio())
                            + ", gcRatio=" + pct(result.resources().avgGcRatio()),
                    Confidence.MEDIUM,
                    "Applies to queue-level capacity decisions.",
                    "Requires external platform metrics for final CPU/network attribution."));
        }

        if (avgUtil <= thresholds.lowUtilization() && completed > 0) {
            recs.add(rec("Q6_IDLE_BUT_SLOW",
                    Recommendation.conf("Do not solve this queue by adding executors; prioritize SQL, layout, leaf parallelism, and plan fixes",
                            "The fixed pool is not consistently busy while queries still consume time.",
                            "Expected to prevent wasting executor resources on non-resource bottlenecks."),
                    "avgUtilization=" + pct(avgUtil),
                    Confidence.MEDIUM,
                    "Applies to the queue-level capacity decision.",
                    "Low utilization may also reflect missing task interval evidence."));
        }

        cluster(result, "R5_SMALL_FILES").ifPresent(c -> {
            if (common(c, result)) {
                recs.add(rec("Q7_COMMON_SMALL_FILES",
                        Recommendation.conf("Compact upstream files and review spark.sql.files.maxPartitionBytes/openCostInBytes/maxPartitionNum",
                                "Repeated small-file scan findings indicate scheduling overhead and tiny input splits across the queue.",
                                "Expected to improve scan-heavy queries."),
                        evidence(c),
                        Confidence.HIGH,
                        coverage(c, completed),
                        caveat(result, c)));
            }
        });

        cluster(result, "R6_GC_PRESSURE").ifPresent(c -> {
            if (common(c, result)) {
                recs.add(rec("Q8_COMMON_GC_OBJECT_CHURN",
                        Recommendation.conf("Review object-heavy UDFs, vectorization/codegen fallbacks, cache layout, then tune memory or GC",
                                "Repeated GC findings indicate JVM overhead is a queue-wide symptom, not an isolated query.",
                                "Expected to help GC-heavy slow queries."),
                        evidence(c),
                        Confidence.MEDIUM,
                        coverage(c, completed),
                        caveat(result, c)));
            }
        });

        if (!result.contention().starvationWindows().isEmpty()) {
            recs.add(rec("Q9_FAIRNESS_OR_STARVATION",
                    Recommendation.conf("Separate short and long queries with FAIR pools, minShare/weight, or concurrency controls",
                            "Some queries get a very small core share during saturated windows, which is a starvation signal.",
                            "Expected to improve short-query tail latency."),
                    "starvationWindows=" + result.contention().starvationWindows().size(),
                    Confidence.MEDIUM,
                    "Targets contention-limited short queries.",
                    "Requires scheduler mode/pool data to distinguish FIFO head-of-line blocking from FAIR share policy."));
        }

        cluster(result, "R7_BROADCAST_JOIN").ifPresent(c -> {
            if (common(c, result)) {
                recs.add(rec("Q10_STATS_CBO_OR_JOIN_STRATEGY",
                        Recommendation.sql("Refresh table/column stats and verify EXPLAIN COST/runtime sizeInBytes before changing join thresholds",
                                "Repeated broadcast-join opportunities without stats evidence often mean planner inputs are missing or stale.",
                                "Expected to improve join strategy choices across repeated templates."),
                        evidence(c),
                        Confidence.MEDIUM,
                        coverage(c, completed),
                        caveat(result, c)
                                + " Join type, build side legality, hints, AQE final plan, broadcast timeout, and memory fit must be verified."));
            }
        });

        List<QueueAnalysisResult.BottleneckCluster> mechanismClusters = mechanismClusters(result);
        if (!mechanismClusters.isEmpty()) {
            recs.add(rec("Q11_QUEUE_EXECUTION_MECHANISM_GAPS",
                    Recommendation.sql("Audit pushdown/vectorization/DPP/runtime filters/AQE join conversion and SHJ conversion on repeated slow templates",
                            "Repeated plan and shuffle symptoms suggest mechanism-level opportunities that cannot be solved by one generic capacity knob.",
                            "Expected to convert queue advice from symptoms into plan fixes."),
                    mechanismEvidence(mechanismClusters),
                    Confidence.LOW,
                    "Applies to repeated templates represented in the queue evidence.",
                    caveat(result, mechanismClusters.toArray(new QueueAnalysisResult.BottleneckCluster[0]))
                            + " This is a mechanism checklist; confirm with SQL UI operator metrics and final adaptive plans."));
        }

        cluster(result, "R8_SCHEDULING_DELAY").ifPresent(c -> {
            if (common(c, result)) {
                recs.add(rec("Q12_SCHEDULING_COLD_START_OR_POOL_DELAY",
                        Recommendation.conf("Check dynamic allocation warm-up, scheduler pools, and pre-allocated minimum executors",
                                "Repeated stage pre-launch gaps mean SQLs are spending time before tasks start; this may be executor cold start, FIFO/FAIR pool delay, or resource acquisition latency.",
                                "Expected to improve latency for short and bursty queries."),
                        evidence(c),
                        Confidence.MEDIUM,
                        coverage(c, completed),
                        caveat(result, c)
                                + " Event logs expose this as firstTaskLaunch - stageSubmission, not a direct queue-wait metric."));
            }
        });

        Optional<QueueAnalysisResult.BottleneckCluster> retryCluster = cluster(result, "R10_TASK_RETRY");
        boolean highAttempts = result.resources().failedAttemptRatio() >= thresholds.highAttemptRatio()
                || result.resources().speculativeAttemptRatio() >= thresholds.highAttemptRatio();
        if ((retryCluster.isPresent() && common(retryCluster.get(), result)) || highAttempts) {
            String evidence = retryCluster.isPresent()
                    ? evidence(retryCluster.get())
                    : "failedAttemptRatio=" + pct(result.resources().failedAttemptRatio())
                    + ", speculativeAttemptRatio=" + pct(result.resources().speculativeAttemptRatio());
            recs.add(rec("Q13_QUEUE_RETRY_OR_SPECULATION_NOISE",
                    Recommendation.conf("Separate failed retries from speculative duplicate attempts before tuning partitions or capacity",
                            "Repeated attempts add wall-clock noise and can make resource or partition advice look worse than the underlying plan.",
                            "Expected to stabilize the queue and make later SQL tuning evidence trustworthy."),
                    evidence,
                    Confidence.MEDIUM,
                    "Applies to queue-level reliability decisions.",
                    "Inspect TaskEndReason, executor/container health, speculation settings, and noisy hosts before changing SQL knobs."));
        }

        QueueAnalysisResult.TemplateStat hotTemplate = highFrequencyTemplate(result);
        if (hotTemplate != null) {
            recs.add(rec("Q14_HIGH_FREQUENCY_TEMPLATE_COST",
                    Recommendation.sql("Prioritize the repeated template by total cost, not only the single slowest execution",
                            "A moderately slow SQL template can dominate queue cost when it runs frequently.",
                            "Expected to reduce aggregate queue load and tail latency if the template is reusable."),
                    templateEvidence(hotTemplate, result),
                    Confidence.MEDIUM,
                    "Targets a repeated template represented by templateHash=" + hotTemplate.templateHash() + ".",
                    "Template hash is normalized from SQL text; verify that executions are semantically comparable before applying one fix."));
        }

        QueueAnalysisResult.TemplateStat repeatedScan = repeatedLargeScanTemplate(result);
        if (repeatedScan != null) {
            recs.add(rec("Q15_REPEATED_LARGE_SCAN_CACHE_OR_MATERIALIZE",
                    Recommendation.sql("Evaluate cache, materialized intermediate results, or upstream aggregation for the repeated large-scan template",
                            "The same template repeatedly scans substantial input; reusing hot intermediates may beat per-query micro-tuning.",
                            "Expected to reduce repeated scan and shuffle work when data freshness and memory budget allow it."),
                    templateEvidence(repeatedScan, result),
                    Confidence.LOW,
                    "Applies only when the repeated template reads the same logical data slice.",
                    "Confirm table/path identity, freshness requirements, cache capacity, and eviction risk before enabling cache or materialization."));
        }

        return recs;
    }

    private boolean common(QueueAnalysisResult.BottleneckCluster c, QueueAnalysisResult result) {
        int baseline = c.scope().contains("FULL_QUEUE")
                ? Math.max(1, result.summary().completedQueries())
                : Math.max(1, result.meta().deepAnalyzedQueries());
        return c.affectedQueries() >= Math.min(thresholds.minAnalyzedQueries(), baseline)
                || c.affectedPct() >= thresholds.commonBottleneckPct();
    }

    private Optional<QueueAnalysisResult.BottleneckCluster> cluster(QueueAnalysisResult r, String ruleId) {
        return r.bottlenecks().stream()
                .filter(c -> c.ruleId().equals(ruleId))
                .findFirst();
    }

    private List<QueueAnalysisResult.BottleneckCluster> mechanismClusters(QueueAnalysisResult result) {
        List<QueueAnalysisResult.BottleneckCluster> clusters = new ArrayList<QueueAnalysisResult.BottleneckCluster>();
        for (String ruleId : new String[]{"R7_BROADCAST_JOIN", "R9_SHUFFLE_FETCH_WAIT", "R11_SORT_AGG_SPILL"}) {
            Optional<QueueAnalysisResult.BottleneckCluster> cluster = cluster(result, ruleId);
            if (cluster.isPresent() && common(cluster.get(), result)) {
                clusters.add(cluster.get());
            }
        }
        return clusters;
    }

    private QueueAnalysisResult.TemplateStat highFrequencyTemplate(QueueAnalysisResult result) {
        QueueAnalysisResult.TemplateStat best = null;
        long totalCoreMs = totalTemplateCoreMs(result);
        long totalDurationMs = totalTemplateDurationMs(result);
        for (QueueAnalysisResult.TemplateStat stat : result.templateStats()) {
            if (stat.queryCount() < thresholds.highFrequencyTemplateMinQueries()) {
                continue;
            }
            double countPct = result.summary().completedQueries() <= 0 ? 0.0
                    : (double) stat.queryCount() / (double) result.summary().completedQueries();
            double corePct = totalCoreMs <= 0L ? 0.0 : (double) stat.totalCoreMs() / (double) totalCoreMs;
            double durationPct = totalDurationMs <= 0L ? 0.0
                    : (double) stat.totalDurationMs() / (double) totalDurationMs;
            boolean trips = countPct >= thresholds.highFrequencyTemplatePct()
                    || corePct >= thresholds.highFrequencyTemplateCostPct()
                    || durationPct >= thresholds.highFrequencyTemplateCostPct();
            if (trips && (best == null || stat.totalCoreMs() > best.totalCoreMs())) {
                best = stat;
            }
        }
        return best;
    }

    private QueueAnalysisResult.TemplateStat repeatedLargeScanTemplate(QueueAnalysisResult result) {
        QueueAnalysisResult.TemplateStat best = null;
        for (QueueAnalysisResult.TemplateStat stat : result.templateStats()) {
            if (stat.queryCount() >= thresholds.highFrequencyTemplateMinQueries()
                    && stat.totalInputBytes() >= thresholds.repeatedScanInputBytes()
                    && (best == null || stat.totalInputBytes() > best.totalInputBytes())) {
                best = stat;
            }
        }
        return best;
    }

    private long totalTemplateCoreMs(QueueAnalysisResult result) {
        long total = 0L;
        for (QueueAnalysisResult.TemplateStat stat : result.templateStats()) {
            total += stat.totalCoreMs();
        }
        return total;
    }

    private long totalTemplateDurationMs(QueueAnalysisResult result) {
        long total = 0L;
        for (QueueAnalysisResult.TemplateStat stat : result.templateStats()) {
            total += stat.totalDurationMs();
        }
        return total;
    }

    private QueueAnalysisResult.QueueRecommendation rec(String id, Recommendation recommendation,
                                                        String evidence, Confidence confidence,
                                                        String expectedCoverage) {
        return rec(id, recommendation, evidence, confidence, expectedCoverage, "");
    }

    private QueueAnalysisResult.QueueRecommendation rec(String id, Recommendation recommendation,
                                                        String evidence, Confidence confidence,
                                                        String expectedCoverage, String caveats) {
        return new QueueAnalysisResult.QueueRecommendation(
                id, recommendation, evidence, confidence, expectedCoverage, caveats);
    }

    private String evidence(QueueAnalysisResult.BottleneckCluster c) {
        String population = c.scope().contains("FULL_QUEUE") ? "completed queries" : "deep-analyzed queries";
        return c.ruleId() + " affected " + c.affectedQueries() + " " + population + " ("
                + pct(c.affectedPct()) + "), scope=" + c.scope()
                + ", sampleCoverage=" + pct(c.sampleCoveragePct());
    }

    private String mechanismEvidence(List<QueueAnalysisResult.BottleneckCluster> clusters) {
        List<String> parts = new ArrayList<String>();
        for (QueueAnalysisResult.BottleneckCluster cluster : clusters) {
            parts.add(evidence(cluster));
        }
        return String.join("; ", parts);
    }

    private String templateEvidence(QueueAnalysisResult.TemplateStat stat, QueueAnalysisResult result) {
        double countPct = result.summary().completedQueries() <= 0 ? 0.0
                : (double) stat.queryCount() / (double) result.summary().completedQueries();
        double corePct = totalTemplateCoreMs(result) <= 0L ? 0.0
                : (double) stat.totalCoreMs() / (double) totalTemplateCoreMs(result);
        return "templateHash=" + stat.templateHash()
                + ", exampleStatementId=" + stat.exampleStatementId()
                + ", queryCount=" + stat.queryCount()
                + " (" + pct(countPct) + ")"
                + ", totalDurationMs=" + stat.totalDurationMs()
                + ", totalCoreMs=" + stat.totalCoreMs()
                + " (" + pct(corePct) + " of tracked template core-ms)"
                + ", totalInputBytes=" + stat.totalInputBytes()
                + ", totalShuffleReadBytes=" + stat.totalShuffleReadBytes();
    }

    private String coverage(QueueAnalysisResult.BottleneckCluster c, int completed) {
        if (completed <= 0) {
            return "No completed queries in this snapshot.";
        }
        if (c.scope().contains("FULL_QUEUE")) {
            return "At least " + c.affectedQueries() + " of " + completed
                    + " completed queries show this evidence in full-queue light metrics.";
        }
        return "At least " + c.affectedQueries() + " of " + completed
                + " completed queries show this evidence in the analyzed deep sample.";
    }

    private String caveat(QueueAnalysisResult result, QueueAnalysisResult.BottleneckCluster... clusters) {
        boolean hasFullQueueLight = false;
        for (QueueAnalysisResult.BottleneckCluster cluster : clusters) {
            hasFullQueueLight = hasFullQueueLight || cluster.scope().contains("FULL_QUEUE");
        }
        if (hasFullQueueLight) {
            return "Light metrics cover all completed SQL executions; operator-level attribution still requires drilldown.";
        }
        return "Deep findings cover " + pct(result.meta().deepCoveragePct())
                + " of SQL executions; low coverage reduces confidence.";
    }

    private static String pct(double v) {
        return String.format("%.1f%%", v * 100.0);
    }
}
