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
 * R2 — Excessive spill. Triggers when a stage spills a large amount relative to its reducer
 * input. The ratio is strongest for reduce-side spill; when shuffle read is small or zero this
 * rule deliberately reports a lower-confidence operator-spill symptom instead of pretending the
 * reducer denominator is exact.
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
            boolean reduceSide = st.shuffleReadBytes() > 0L;
            Map<String, String> evidence = new LinkedHashMap<>();
            evidence.put("spillBytes", String.valueOf(spill));
            evidence.put("shuffleReadBytes", String.valueOf(st.shuffleReadBytes()));
            evidence.put("spillRatio", String.format("%.2f", spillRatio));
            evidence.put("spillSubtype", reduceSide ? "REDUCE_SIDE_SPILL" : "OPERATOR_SPILL_HINT");
            evidence.put("memoryOverheadPrimaryFix", "false");

            String explanation = String.format(
                    reduceSide
                            ? "Stage %d spills %.0f%% of its shuffle-read volume; reducer partitions may be too large for the per-task execution memory budget."
                            : "Stage %d spills with little or no shuffle-read volume; this is an operator-level spill symptom, so the shuffle-read ratio is only a heuristic.",
                    st.stageId(), spillRatio * 100);

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
                    "increase spark.executor.memory only after ruling out skew or oversized partitions",
                    "JVM SQL spill is usually governed by execution memory and per-task data size; executor memory can help, but only after partition shape is sane.",
                    "Do not treat memoryOverhead as the default fix unless PySpark/native/off-heap/container evidence exists."));

            findings.add(new Finding(id(), "spill", Severity.WARN, st.stageId(),
                    explanation, evidence, recs));
        }
        return findings;
    }
}
