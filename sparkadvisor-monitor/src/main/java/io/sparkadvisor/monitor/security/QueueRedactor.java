package io.sparkadvisor.monitor.security;

import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;

import java.util.regex.Pattern;

/**
 * Redacts queue-report fields before JSON rendering or LLM prompts.
 *
 * <p>The queue contract intentionally avoids raw event-log lines and full SQL text. The remaining
 * sensitive fields are mostly paths and secret-like substrings in metadata; keep this conservative
 * and deterministic so the same report shape is used by CLI, HTML, SHS, and LLM advisor.
 */
public final class QueueRedactor {

    private static final Pattern SECRET_ASSIGNMENT =
            Pattern.compile("(?i)(password|passwd|secret|token|access[._-]?key|keytab|credential)(=|:)[^,;\\s]+");
    private static final Pattern URI_AUTHORITY =
            Pattern.compile("(?i)(hdfs|s3a?|obs|oss|abfs|wasbs?)://([^/]+)");

    private QueueRedactor() {}

    public static QueueAnalysisResult redact(QueueAnalysisResult result) {
        if (result == null || result.meta() == null) {
            return result;
        }
        QueueAnalysisResult.Meta m = result.meta();
        QueueAnalysisResult.Meta redactedMeta = new QueueAnalysisResult.Meta(
                m.sparkAdvisorVersion(),
                m.generatedAt(),
                m.incomplete(),
                m.runningSnapshot(),
                m.incremental(),
                redactText(m.sourcePath()),
                redactText(m.snapshotKey()),
                m.deepAnalyzedTopN(),
                m.lightAnalyzedQueries(),
                m.deepAnalyzedQueries(),
                m.deepCoveragePct(),
                m.samplingStrategy(),
                "DEFAULT_REDACTION",
                redactText(m.degradedReason()),
                redactText(m.assumptions()));
        return new QueueAnalysisResult(
                result.summary(),
                result.timeline(),
                result.bottlenecks(),
                result.utilization(),
                result.resources(),
                result.contention(),
                result.topSlowQueries(),
                result.sampledQueries(),
                result.templateStats(),
                result.globalRecommendations(),
                result.aiAdvice(),
                redactedMeta);
    }

    public static String redactText(String value) {
        if (value == null) {
            return null;
        }
        String out = URI_AUTHORITY.matcher(value).replaceAll("$1://<redacted-authority>");
        out = SECRET_ASSIGNMENT.matcher(out).replaceAll("$1$2<redacted>");
        return out;
    }
}
