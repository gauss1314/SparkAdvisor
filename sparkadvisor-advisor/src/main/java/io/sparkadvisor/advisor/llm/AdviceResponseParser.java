package io.sparkadvisor.advisor.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.report.model.AnalysisResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses the LLM's JSON response into an {@link AnalysisResult.AiAdvice}.
 *
 * <p>Robust to common model quirks: strips ```json fences and any prose before/after the JSON
 * object, and tolerates missing fields. On unparseable input it returns advice carrying the
 * raw text as the summary (so the user still sees something) rather than throwing.
 */
public final class AdviceResponseParser {

    private final ObjectMapper mapper = new ObjectMapper();

    public AnalysisResult.AiAdvice parse(String provider, String raw) {
        String json = extractJson(raw);
        try {
            JsonNode root = mapper.readTree(json);
            String summary = root.path("summary").asText("");
            List<Recommendation> recs = new ArrayList<>();
            JsonNode arr = root.path("recommendations");
            if (arr.isArray()) {
                for (JsonNode n : arr) {
                    Recommendation.Type type = parseType(n.path("type").asText("SPARK_CONF"));
                    recs.add(new Recommendation(
                            type,
                            n.path("action").asText(""),
                            n.path("rationale").asText(""),
                            n.path("expectedImpact").asText(null)));
                }
            }
            if (summary.trim().isEmpty() && recs.isEmpty()) {
                // Parsed but empty -> treat raw as summary.
                return new AnalysisResult.AiAdvice(provider, raw == null ? "" : raw.trim(), new java.util.ArrayList<Recommendation>());
            }
            return new AnalysisResult.AiAdvice(provider, summary, recs);
        } catch (Exception e) {
            // Unparseable -> degrade gracefully, surface the raw text.
            return new AnalysisResult.AiAdvice(provider,
                    raw == null ? "(no response)" : raw.trim(), new java.util.ArrayList<Recommendation>());
        }
    }

    private Recommendation.Type parseType(String s) {
        try {
            return Recommendation.Type.valueOf(s.trim().toUpperCase());
        } catch (RuntimeException e) {
            return Recommendation.Type.SPARK_CONF;
        }
    }

    /** Extract the first {...} JSON object, stripping code fences and surrounding prose. */
    public static String extractJson(String raw) {
        if (raw == null) return "{}";
        String s = raw.trim();
        // Strip ``` or ```json fences.
        s = s.replace("```json", "").replace("```", "").trim();
        int start = s.indexOf('{');
        int end = s.lastIndexOf('}');
        if (start >= 0 && end > start) {
            return s.substring(start, end + 1);
        }
        return "{}";
    }
}
