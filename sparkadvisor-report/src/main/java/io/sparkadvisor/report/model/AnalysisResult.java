package io.sparkadvisor.report.model;

import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.predict.ExecutorScalingPrediction;
import io.sparkadvisor.core.predict.ShufflePartitionPrediction;

import java.util.List;

/**
 * The universal contract of SparkAdvisor. The CLI, the UI, and (future) the LLM advisor
 * all consume this single structure. HTML is just one rendering of it.
 *
 * <p>Deliberately free of any Spark types so it serializes cleanly to JSON and keeps the
 * report module decoupled from core's provided Spark dependency.
 *
 * @param app              application-level summary
 * @param targetSql        the SQL that was located/analyzed (null if app-level only)
 * @param findings         rule-engine findings (analyzer)
 * @param shufflePrediction shuffle-partition cost-model prediction (nullable)
 * @param executorPrediction executor-scaling prediction (nullable)
 * @param aiAdvice         LLM advisor output (null until F4 is implemented)
 * @param meta             provenance and confidence info
 */
public record AnalysisResult(
        AppSummary app,
        SqlAnalysis targetSql,
        List<Finding> findings,
        ShufflePartitionPrediction shufflePrediction,
        ExecutorScalingPrediction executorPrediction,
        AiAdvice aiAdvice,
        Meta meta) {

    /** Return a copy with the AI advice block populated (records are immutable). */
    public AnalysisResult withAiAdvice(AiAdvice advice) {
        return new AnalysisResult(app, targetSql, findings, shufflePrediction,
                executorPrediction, advice, meta);
    }

    /** Application-level summary. */
    public record AppSummary(
            String appId,
            String appName,
            long durationMs,
            int sqlExecutionCount,
            int jobCount,
            int stageCount,
            int availableCores) {
    }

    /** Provenance and confidence. */
    public record Meta(
            String sparkAdvisorVersion,
            String generatedAt,
            boolean incomplete,
            String sourcePath) {
    }

    /** Placeholder for F4 LLM output; null until implemented. */
    public record AiAdvice(
            String provider,
            String summary,
            List<Recommendation> recommendations) {
    }
}
