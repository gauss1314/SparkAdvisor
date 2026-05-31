package io.sparkadvisor.analyzer.rule;

import io.sparkadvisor.analyzer.RuleContext;
import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.finding.Severity;
import io.sparkadvisor.core.util.Strings;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * R7 — Broadcast join opportunity / fallback (heuristic).
 *
 * <p>The event log does not directly state "a broadcast was rejected", so this is a best-effort
 * heuristic on the physical plan text: if the plan uses {@code SortMergeJoin} (a shuffle join)
 * AND no {@code BroadcastHashJoin} is present, there may be an opportunity to broadcast a small
 * side. We surface this as INFO with explicit caveats — the user must confirm statistics,
 * build-side legality, join type, hints, and AQE final-plan behavior.
 *
 * <p>Reported at SQL level. Skipped entirely when no plan text is available.
 */
public final class BroadcastJoinRule implements Rule {

    @Override
    public String id() {
        return "R7_BROADCAST_JOIN";
    }

    @Override
    public List<Finding> evaluate(RuleContext ctx) {
        SqlAnalysis sql = ctx.sql();
        String plan = sql.physicalPlanText();
        if (Strings.isBlank(plan)) {
            return new java.util.ArrayList<Finding>();
        }
        boolean hasSortMergeJoin = plan.contains("SortMergeJoin");
        boolean hasBroadcastJoin = plan.contains("BroadcastHashJoin")
                || plan.contains("BroadcastNestedLoopJoin");
        if (!hasSortMergeJoin || hasBroadcastJoin) {
            return new java.util.ArrayList<Finding>();
        }

        Map<String, String> evidence = new LinkedHashMap<>();
        evidence.put("planHasSortMergeJoin", "true");
        evidence.put("planHasBroadcastJoin", "false");
        evidence.put("requiresStatsCheck", "true");
        evidence.put("requiresJoinTypeAndBuildSideCheck", "true");
        evidence.put("aqeRuntimeConversionMayApply", "true");
        evidence.put("broadcastTimeoutAndMemoryRisk", "true");

        String explanation =
                "The plan uses a shuffle-based SortMergeJoin with no broadcast join present. "
                        + "This may be a broadcast opportunity only if a build side is legally broadcastable, "
                        + "stats/runtime size prove it is small, hints do not conflict, and AQE did not already "
                        + "convert the final plan.";

        List<Recommendation> recs = new java.util.ArrayList<Recommendation>(java.util.Arrays.asList(
                Recommendation.conf(
                        "verify table stats / runtime sizeInBytes, then tune spark.sql.autoBroadcastJoinThreshold or spark.sql.adaptive.autoBroadcastJoinThreshold",
                        "Spark chooses broadcast joins from size statistics and legal build-side rules; without reliable stats this recommendation is low-confidence.",
                        "Effective only when one side is genuinely small; too high risks broadcast timeout or memory pressure."),
                Recommendation.sql(
                        "add a broadcast() hint only when join type and small-side memory fit are verified",
                        "Hints can override planner choices, but incorrect hints may force an unsafe broadcast.",
                        "Use after confirming EXPLAIN COST / runtime statistics and build side.")));

        java.util.List<Finding> out = new java.util.ArrayList<Finding>();
        out.add(new Finding(id(), "join", Severity.INFO, null, explanation, evidence, recs));
        return out;
    }
}
