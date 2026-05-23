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
        int analyzed = Math.max(1, result.meta().deepAnalyzedTopN());

        cluster(result, "R2_EXCESSIVE_SPILL").ifPresent(c -> {
            if (common(c, analyzed)) {
                recs.add(rec("Q1_COMMON_SPILL",
                        Recommendation.conf("Increase per-task memory headroom or reduce shuffle partition size",
                                "A recurring spill pattern means many queue queries materialize partitions larger than available execution memory.",
                                "Expected to help queries represented by the repeated R2 findings."),
                        evidence(c),
                        Confidence.MEDIUM,
                        coverage(c, completed)));
            }
        });

        cluster(result, "R1_DATA_SKEW").ifPresent(c -> {
            if (common(c, analyzed)) {
                recs.add(rec("Q2_COMMON_SKEW",
                        Recommendation.conf("Audit AQE skew-join settings and skewed join keys",
                                "Repeated skew findings are usually key-distribution issues; changing only shuffle.partitions is unlikely to fix them.",
                                "Expected to help skew-limited slow queries."),
                        evidence(c),
                        Confidence.HIGH,
                        coverage(c, completed)));
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
                    Recommendation.conf("Use AQE coalescing/advisoryPartitionSizeInBytes instead of a single static shuffle.partitions value",
                            "The queue contains both under-partitioned large queries and over-partitioned small queries, so one static partition count is fighting itself.",
                            "Expected to improve mixed workloads without penalizing small queries as much."),
                    evidence(lowParallel.get()) + "; " + evidence(overParallel.get()),
                    Confidence.HIGH,
                    "Covers both affected groups in the analyzed top queries."));
        }

        double avgUtil = result.utilization().avgUtilization();
        if (avgUtil >= thresholds.highUtilization()
                && result.contention().contentionLimitedPct() >= thresholds.contentionLimitedPct()) {
            recs.add(rec("Q4_RESOURCE_CONTENTION",
                    Recommendation.conf("Increase fixed queue capacity or isolate/limit large resource-hog queries",
                            "The shared executor pool is saturated and many slow queries appear contention-limited.",
                            "Expected to reduce latency for contention-limited queries."),
                    "avgUtilization=" + pct(avgUtil)
                            + ", contentionLimitedPct=" + pct(result.contention().contentionLimitedPct()),
                    Confidence.MEDIUM,
                    pct(result.contention().contentionLimitedPct()) + " of completed queries were classified as contention-limited."));
        }

        if (avgUtil <= thresholds.lowUtilization() && completed > 0) {
            recs.add(rec("Q5_LOW_POOL_UTILIZATION",
                    Recommendation.conf("Do not solve this queue by adding executors; prioritize SQL, layout, and plan fixes",
                            "The fixed pool is not consistently busy while queries still consume time.",
                            "Expected to prevent wasting executor resources on non-resource bottlenecks."),
                    "avgUtilization=" + pct(avgUtil),
                    Confidence.MEDIUM,
                    "Applies to the queue-level capacity decision."));
        }

        cluster(result, "R5_SMALL_FILES").ifPresent(c -> {
            if (common(c, analyzed)) {
                recs.add(rec("Q6_COMMON_SMALL_FILES",
                        Recommendation.conf("Compact upstream files and review spark.sql.files.maxPartitionBytes",
                                "Repeated small-file scan findings indicate scheduling overhead and tiny input splits across the queue.",
                                "Expected to improve scan-heavy queries."),
                        evidence(c),
                        Confidence.HIGH,
                        coverage(c, completed)));
            }
        });

        cluster(result, "R6_GC_PRESSURE").ifPresent(c -> {
            if (common(c, analyzed)) {
                recs.add(rec("Q7_COMMON_GC",
                        Recommendation.conf("Review executor memory, GC collector settings, serialization, and heavy UDF object churn",
                                "Repeated GC findings indicate JVM overhead is a queue-wide symptom, not an isolated query.",
                                "Expected to help GC-heavy slow queries."),
                        evidence(c),
                        Confidence.MEDIUM,
                        coverage(c, completed)));
            }
        });

        return recs;
    }

    private boolean common(QueueAnalysisResult.BottleneckCluster c, int analyzed) {
        return c.affectedQueries() >= Math.min(thresholds.minAnalyzedQueries(), analyzed)
                || c.affectedPct() >= thresholds.commonBottleneckPct();
    }

    private Optional<QueueAnalysisResult.BottleneckCluster> cluster(QueueAnalysisResult r, String ruleId) {
        return r.bottlenecks().stream()
                .filter(c -> c.ruleId().equals(ruleId))
                .findFirst();
    }

    private QueueAnalysisResult.QueueRecommendation rec(String id, Recommendation recommendation,
                                                        String evidence, Confidence confidence,
                                                        String expectedCoverage) {
        return new QueueAnalysisResult.QueueRecommendation(
                id, recommendation, evidence, confidence, expectedCoverage);
    }

    private String evidence(QueueAnalysisResult.BottleneckCluster c) {
        return c.ruleId() + " affected " + c.affectedQueries() + " analyzed queries ("
                + pct(c.affectedPct()) + ")";
    }

    private String coverage(QueueAnalysisResult.BottleneckCluster c, int completed) {
        if (completed <= 0) {
            return "No completed queries in this snapshot.";
        }
        return "At least " + c.affectedQueries() + " of " + completed
                + " completed queries show this evidence in the analyzed slow-query set.";
    }

    private static String pct(double v) {
        return String.format("%.1f%%", v * 100.0);
    }
}
