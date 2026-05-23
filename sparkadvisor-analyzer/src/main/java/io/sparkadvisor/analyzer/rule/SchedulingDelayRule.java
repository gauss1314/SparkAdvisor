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
 * R8 — Scheduling wait. Triggers when the gap between stage submission and the first task
 * launch is a large fraction of the stage wall clock, indicating resource/scheduling wait
 * (e.g. dynamic-allocation cold start).
 */
public final class SchedulingDelayRule implements Rule {

    @Override
    public String id() {
        return "R8_SCHEDULING_DELAY";
    }

    @Override
    public List<Finding> evaluate(RuleContext ctx) {
        List<Finding> findings = new ArrayList<>();
        double warn = ctx.thresholds().schedulingDelayRatioWarn();
        for (StageAnalysis st : ctx.sql().stages()) {
            long wall = st.wallClockMs();
            long delay = st.schedulingDelayMs();
            if (wall <= 0 || delay <= 0) {
                continue;
            }
            double ratio = (double) delay / (double) wall;
            if (ratio < warn) {
                continue;
            }
            Map<String, String> evidence = new LinkedHashMap<>();
            evidence.put("schedulingDelayMs", String.valueOf(delay));
            evidence.put("stageWallMs", String.valueOf(wall));
            evidence.put("delayRatio", String.format("%.2f", ratio));

            String explanation = String.format(
                    "Stage %d waited %.0f%% of its time before the first task launched, "
                            + "indicating resource/scheduling wait.", st.stageId(), ratio * 100);

            List<Recommendation> recs = List.of(
                    Recommendation.conf(
                            "warm up executors (raise spark.dynamicAllocation.minExecutors) "
                                    + "or pre-allocate resources",
                            "Cold-start executor acquisition delays the first tasks.",
                            "Trades idle capacity for lower latency."));

            findings.add(new Finding(id(), "scheduling", Severity.INFO, st.stageId(),
                    explanation, evidence, recs));
        }
        return findings;
    }
}
