package io.sparkadvisor.monitor.aggregate;

import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.predict.Confidence;
import io.sparkadvisor.report.model.AnalysisResult;

import java.util.List;

/**
 * Queue-level JSON contract. It is intentionally Spark-type-free and sits beside the single
 * SQL {@link AnalysisResult} contract.
 */
public record QueueAnalysisResult(
        QueueSummary summary,
        List<HourBucketStat> timeline,
        List<BottleneckCluster> bottlenecks,
        UtilizationSeries utilization,
        ResourceMetrics resources,
        ContentionReport contention,
        List<SlowQueryRef> topSlowQueries,
        List<QueueRecommendation> globalRecommendations,
        AnalysisResult.AiAdvice aiAdvice,
        Meta meta) {

    public QueueAnalysisResult withRecommendations(List<QueueRecommendation> recommendations) {
        return new QueueAnalysisResult(summary, timeline, bottlenecks, utilization, resources,
                contention, topSlowQueries, recommendations, aiAdvice, meta);
    }

    public record QueueSummary(
            String appId,
            String appName,
            long windowStart,
            long windowEnd,
            int totalQueries,
            int completedQueries,
            int runningQueries,
            int failedQueries,
            int fixedExecutorCores) {
    }

    public record HourBucketStat(
            long bucketStart,
            long bucketEnd,
            int queryCount,
            long p50Ms,
            long p95Ms,
            long p99Ms,
            double avgUtilization) {
    }

    public record BottleneckCluster(
            String ruleId,
            String category,
            int affectedQueries,
            double affectedPct) {
    }

    public record UtilizationSeries(
            List<Point> points,
            double avgUtilization,
            double peakUtilization) {
        public record Point(long bucketStart, long bucketEnd, double avgUtilization) {}
    }

    public record ResourceMetrics(
            long totalSpillBytes,
            double avgMaxGcRatio,
            double p95MaxGcRatio,
            double maxGcRatio) {
    }

    public record ContentionReport(
            double contentionLimitedPct,
            List<Window> hotspots,
            List<SlowQueryRef> topResourceHogs) {
        public record Window(long startTime, long endTime, double avgUtilization) {}
    }

    public record SlowQueryRef(
            String statementId,
            long executionId,
            long startTime,
            long endTime,
            long durationMs,
            String dominantBottleneck,
            boolean contentionLimited,
            long ownCoreMs) {
    }

    public record QueueRecommendation(
            String queueRuleId,
            Recommendation recommendation,
            String evidence,
            Confidence confidence,
            String expectedCoverage) {
    }

    public record Meta(
            String sparkAdvisorVersion,
            String generatedAt,
            boolean incomplete,
            boolean runningSnapshot,
            String sourcePath,
            int deepAnalyzedTopN,
            String assumptions) {
    }
}
