package io.sparkadvisor.monitor.advisor;

import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;
import io.sparkadvisor.monitor.render.QueueJsonWriter;
import io.sparkadvisor.report.i18n.ReportLanguage;

/**
 * Builds queue-level prompts for LLM advice.
 *
 * <p>The model receives only the structured {@link QueueAnalysisResult} JSON: aggregated
 * latency distributions, bottleneck clusters, contention evidence, global queue rules, and
 * slow-query references. It never receives raw event log lines.
 */
public final class QueuePromptBuilder {

    private final QueueJsonWriter jsonWriter = new QueueJsonWriter();
    private final ReportLanguage language;

    public QueuePromptBuilder() {
        this(ReportLanguage.EN);
    }

    public QueuePromptBuilder(ReportLanguage language) {
        this.language = language == null ? ReportLanguage.EN : language;
    }

    public String systemPrompt() {
        return String.join("\n",
                "You are a senior Apache Spark performance engineer advising on a long-running",
                "shared Spark SQL query queue. You will be given a STRUCTURED queue-level analysis",
                "summary (JSON), already produced by a deterministic analyzer. It includes latency",
                "trends, utilization, contention evidence, repeated bottleneck clusters, top slow",
                "queries, and global rule-based recommendations. You do NOT have the raw event log",
                "and do not need it. Reason only from the provided summary.",
                "",
                "Your job:",
                "1. Explain the queue-level root cause in plain language, distinguishing contention,",
                "   repeated per-query bottlenecks, and fixed-resource-pool saturation.",
                "2. Give concrete, prioritized global tuning actions for the queue/application.",
                "3. Respect evidence thresholds and uncertainty. Do not overstate causality when the",
                "   report says contention or memory pressure is inferred.",
                "4. Prefer global Spark configuration or queue policy changes; mention SQL rewrites",
                "   only when repeated slow-query patterns justify them.",
                language.isChinese()
                        ? "5. Use Simplified Chinese for user-facing prose; keep Spark config keys, enum values, and parameter names in their original English form."
                        : "5. Use English for user-facing prose.",
                "",
                "Respond with ONLY a JSON object, no prose around it, of exactly this shape:",
                "{",
                "  \"summary\": \"<2-4 sentence queue-level root-cause narrative>\",",
                "  \"recommendations\": [",
                "    {\"type\": \"SPARK_CONF\" | \"SQL_REWRITE\",",
                "     \"action\": \"<concrete change>\",",
                "     \"rationale\": \"<why it helps, referencing queue metrics>\",",
                "     \"expectedImpact\": \"<qualitative or rough quantitative effect>\"}",
                "  ]",
                "}",
                "");
    }

    public String userPrompt(QueueAnalysisResult result) {
        String json;
        try {
            json = jsonWriter.toJson(result);
        } catch (Exception e) {
            json = "{\"error\":\"failed to serialize queue analysis\"}";
        }
        return "Here is the structured queue-level analysis summary to reason over:\n\n" + json;
    }
}
