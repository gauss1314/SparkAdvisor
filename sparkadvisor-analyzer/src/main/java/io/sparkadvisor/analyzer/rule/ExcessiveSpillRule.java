package io.sparkadvisor.analyzer.rule;

import io.sparkadvisor.analyzer.RuleContext;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.finding.Severity;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * R2 — Excessive spill. Triggers when a stage spills a large fraction of its shuffle-read
 * volume to memory/disk, indicating partitions too large for the per-task memory budget.
 *
 * <p>AQE-aware: the right knob under AQE coalescing is {@code advisoryPartitionSizeInBytes}
 * (smaller advisory size => more, smaller partitions => less spill), not the static
 * {@code spark.sql.shuffle.partitions}.
 */
public final class ExcessiveSpillRule implements Rule {

    @Override
    public String id() {
        return "R2_EXCESSIVE_SPILL";
    }

    @Override
    public List<Finding> evaluate(RuleContext ctx) {
        List<Finding> findings = new ArrayList<>();
        double warn = ctx.thresholds().spillRatioWarn();
        for (StageAnalysis st : ctx.sql().stages()) {
            long spill = st.spillBytes();
            if (spill <= 0) {
                continue;
            }
            long basis = Math.max(st.shuffleReadBytes(), 1);
            double spillRatio = (double) spill / (double) basis;
            if (spillRatio < warn) {
                continue;
            }
            Map<String, String> evidence = new LinkedHashMap<>();
            evidence.put("spillBytes", String.valueOf(spill));
            evidence.put("shuffleReadBytes", String.valueOf(st.shuffleReadBytes()));
            evidence.put("spillRatio", String.format("%.2f", spillRatio));

            String explanation = String.format(
                    "Stage %d spills %.0f%% of its shuffle-read volume; partitions are too "
                            + "large for the per-task memory budget.", st.stageId(), spillRatio * 100);

            List<Recommendation> recs = new ArrayList<>();
            if (ctx.aqe().aqeEnabled() && ctx.aqe().coalesceEnabled()) {
                recs.add(Recommendation.conf(
                        "lower spark.sql.adaptive.advisoryPartitionSizeInBytes",
                        "Under AQE coalescing this advisory size controls partition size; a "
                                + "smaller value yields more, smaller partitions that fit in memory.",
                        "Directly reduces per-task data and spill."));
            } else {
                recs.add(Recommendation.conf(
                        "increase spark.sql.shuffle.partitions",
                        "More partitions means less data per task, reducing the chance of spill.",
                        "Effective when the stage is not also skewed."));
            }
            recs.add(Recommendation.conf(
                    "increase executor memory (spark.executor.memory / memoryOverhead)",
                    "A larger per-task memory budget lets more of the partition stay in memory.",
                    "Helps spill broadly but costs cluster memory."));

            findings.add(new Finding(id(), "spill", Severity.WARN, st.stageId(),
                    explanation, evidence, recs));
        }
        return findings;
    }
}
