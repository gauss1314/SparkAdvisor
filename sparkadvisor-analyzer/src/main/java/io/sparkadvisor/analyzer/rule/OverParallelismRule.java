package io.sparkadvisor.analyzer.rule;

import io.sparkadvisor.analyzer.RuleContext;
import io.sparkadvisor.analyzer.RuleThresholds;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.finding.Severity;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * R4 — Over-parallelism. Triggers when a stage has a very large task count but the median
 * task is tiny, so per-task fixed overhead (scheduling, launch, deserialize) dominates.
 */
public final class OverParallelismRule implements Rule {

    @Override
    public String id() {
        return "R4_OVER_PARALLELISM";
    }

    @Override
    public List<Finding> evaluate(RuleContext ctx) {
        RuleThresholds t = ctx.thresholds();
        List<Finding> findings = new ArrayList<>();
        for (StageAnalysis st : ctx.sql().stages()) {
            boolean tinyTasks = st.medianTaskMs() > 0 && st.medianTaskMs() < t.smallTaskMedianMs();
            boolean manyTasks = st.numTasks() >= t.overParallelMinTasks();
            if (!(tinyTasks && manyTasks)) {
                continue;
            }
            Map<String, String> evidence = new LinkedHashMap<>();
            evidence.put("numTasks", String.valueOf(st.numTasks()));
            evidence.put("medianTaskMs", String.valueOf(st.medianTaskMs()));

            String explanation = String.format(
                    "Stage %d runs %d tasks with a median of only %d ms each; per-task overhead "
                            + "likely dominates useful work.", st.stageId(), st.numTasks(), st.medianTaskMs());

            List<Recommendation> recs = new ArrayList<>();
            if (ctx.aqe().aqeEnabled() && ctx.aqe().coalesceEnabled()) {
                recs.add(Recommendation.conf(
                        "raise spark.sql.adaptive.advisoryPartitionSizeInBytes",
                        "Larger target partitions mean fewer, bigger tasks under AQE coalescing.",
                        "Reduces scheduling overhead."));
            } else {
                recs.add(Recommendation.conf(
                        "reduce spark.sql.shuffle.partitions, or coalesce() the result",
                        "Fewer partitions means fewer, larger tasks with less fixed overhead.",
                        "Reduces scheduling overhead."));
            }
            findings.add(new Finding(id(), "parallelism", Severity.INFO, st.stageId(),
                    explanation, evidence, recs));
        }
        return findings;
    }
}
