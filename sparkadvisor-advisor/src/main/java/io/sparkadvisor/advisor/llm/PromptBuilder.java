package io.sparkadvisor.advisor.llm;

import io.sparkadvisor.report.json.JsonReportWriter;
import io.sparkadvisor.report.model.AnalysisResult;

/**
 * Builds the prompts sent to the LLM.
 *
 * <p><b>Core design principle</b> (the thesis of SparkAdvisor's whole architecture): the model
 * is fed the <b>structured, deterministic summary</b> — the {@link AnalysisResult} JSON, which
 * already compresses a GB-scale event log down to KB of exact hard metrics, findings, and
 * predictions — and <b>never the raw event log</b>. The deterministic layers do the parsing and
 * arithmetic; the LLM only does interpretation, root-cause narrative, and combined tuning advice.
 *
 * <p>The model is instructed to return a strict JSON object so the response parses into
 * {@code AiAdvice} deterministically.
 */
public final class PromptBuilder {

    private final JsonReportWriter jsonWriter = new JsonReportWriter();

    public String systemPrompt() {
        return String.join("\n",
                "You are a senior Apache Spark performance engineer. You will be given a STRUCTURED",
                "analysis summary (JSON) of one Spark SQL execution, already produced by a",
                "deterministic analyzer: hard metrics, a critical-path breakdown, rule findings, and",
                "cost-model predictions. You do NOT have the raw logs and do not need them — reason",
                "only from the provided summary.",
                "",
                "Your job:",
                "1. Explain the most likely ROOT CAUSE of the slowness in plain language, connecting",
                "   the symptoms (e.g. skew + spill on the same stage => skewed join key).",
                "2. Give concrete, prioritized tuning actions (SQL rewrites and/or Spark configs).",
                "3. Respect what the summary already tells you. In particular: if AQE is enabled, do",
                "   NOT suggest enabling it; if a prediction says a stage is skew-limited, do NOT",
                "   suggest repartitioning as the fix.",
                "4. Be honest about uncertainty — these are estimates.",
                "",
                "Respond with ONLY a JSON object, no prose around it, of exactly this shape:",
                "{",
                "  \"summary\": \"<2-4 sentence root-cause narrative>\",",
                "  \"recommendations\": [",
                "    {\"type\": \"SPARK_CONF\" | \"SQL_REWRITE\",",
                "     \"action\": \"<concrete change>\",",
                "     \"rationale\": \"<why it helps, referencing the metrics>\",",
                "     \"expectedImpact\": \"<qualitative or rough quantitative effect>\"}",
                "  ]",
                "}",
                "");
    }

    /** The user prompt: the structured analysis as JSON. */
    public String userPrompt(AnalysisResult result) {
        String json;
        try {
            json = jsonWriter.toJson(result);
        } catch (Exception e) {
            json = "{\"error\":\"failed to serialize analysis\"}";
        }
        return "Here is the structured analysis summary to reason over:\n\n" + json;
    }
}
