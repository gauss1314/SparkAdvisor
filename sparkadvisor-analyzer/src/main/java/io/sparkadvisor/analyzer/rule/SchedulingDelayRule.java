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
 * R8 — Stage startup / pre-launch gap. Runtime ID remains R8_SCHEDULING_DELAY for JSON
 * compatibility. This derived metric is the gap between stage submission and first task launch;
 * it may indicate resource acquisition, scheduler pool contention, dynamic-allocation cold start,
 * or other pre-launch delay.
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
            evidence.put("derivedMetric", "firstTaskLaunchTime - stageSubmissionTime");
            evidence.put("resourceQueueingDirectMetric", "false");

            String explanation = String.format(
                    "Stage %d spent %.0f%% of its wall time before the first task launched. "
                            + "This is a derived pre-launch gap, not a direct Spark resource-queue metric.",
                    st.stageId(), ratio * 100);

            List<Recommendation> recs = new java.util.ArrayList<Recommendation>(java.util.Arrays.asList(
                    Recommendation.conf(
                            "warm up executors (raise spark.dynamicAllocation.minExecutors) "
                                    + "or pre-allocate resources",
                            "Cold-start executor acquisition delays the first tasks.",
                            "Trades idle capacity for lower latency."),
                    Recommendation.conf(
                            "check scheduler mode, FAIR pools, minShare/weight, and queue contention in the queue report",
                            "A large pre-launch gap can also come from pool contention or FIFO head-of-line blocking.",
                            "Use queue-level evidence before treating it as executor shortage.")));

            findings.add(new Finding(id(), "scheduling", Severity.INFO, st.stageId(),
                    explanation, evidence, recs));
        }
        return findings;
    }
}
