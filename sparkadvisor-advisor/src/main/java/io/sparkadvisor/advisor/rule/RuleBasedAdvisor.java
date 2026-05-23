package io.sparkadvisor.advisor.rule;

import io.sparkadvisor.advisor.api.TuningAdvisor;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.finding.Severity;
import io.sparkadvisor.core.predict.ShufflePartitionPrediction;
import io.sparkadvisor.report.model.AnalysisResult;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Deterministic, offline advisor. Synthesizes a plain-language summary and a de-duplicated,
 * priority-ordered recommendation list from the analysis's findings and predictions.
 *
 * <p>Always available (no network, no API key), so it is the default advisor. The LLM advisor
 * can replace or augment it, returning the same {@link AnalysisResult.AiAdvice} shape.
 */
public final class RuleBasedAdvisor implements TuningAdvisor {

    @Override
    public String name() {
        return "rule-based";
    }

    @Override
    public AnalysisResult.AiAdvice advise(AnalysisResult r) {
        List<Finding> findings = r.findings() == null ? List.of() : r.findings();
        String summary = summarize(r, findings);

        // Consolidate recommendations: criticals first, then warns, de-duplicated by action.
        List<Recommendation> recs = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (Severity sev : new Severity[]{Severity.CRITICAL, Severity.WARN, Severity.INFO}) {
            for (Finding f : findings) {
                if (f.severity() != sev) continue;
                for (Recommendation rec : f.recommendations()) {
                    if (seen.add(rec.action())) {
                        recs.add(rec);
                    }
                }
            }
        }
        return new AnalysisResult.AiAdvice(name(), summary, recs);
    }

    private String summarize(AnalysisResult r, List<Finding> findings) {
        StringBuilder s = new StringBuilder();
        if (r.targetSql() != null) {
            var sql = r.targetSql();
            s.append(String.format(
                    "This SQL ran in %d ms; its critical path is %d ms, so up to %.0f%% of the "
                            + "wall clock is potentially removable. ",
                    sql.wallClockMs(), sql.criticalPathMs(),
                    Math.max(0, sql.deviation()) * 100));
        }
        long crit = findings.stream().filter(f -> f.severity() == Severity.CRITICAL).count();
        long warn = findings.stream().filter(f -> f.severity() == Severity.WARN).count();
        if (findings.isEmpty()) {
            s.append("No rule findings were triggered; the query looks healthy by the current thresholds.");
            return s.toString();
        }
        s.append(String.format("%d critical and %d warning issue(s) were found. ", crit, warn));

        // Lead with the most severe issue's explanation.
        findings.stream()
                .filter(f -> f.severity() == Severity.CRITICAL)
                .findFirst()
                .ifPresent(f -> s.append("Top priority: ").append(f.explanation()).append(" "));

        // Reference the shuffle prediction direction if present.
        ShufflePartitionPrediction sp = r.shufflePrediction();
        if (sp != null && sp.direction() == ShufflePartitionPrediction.Direction.SKEW_LIMITED) {
            s.append("Note: repartitioning is unlikely to help here because the stage is skew-limited.");
        }
        return s.toString().trim();
    }
}
