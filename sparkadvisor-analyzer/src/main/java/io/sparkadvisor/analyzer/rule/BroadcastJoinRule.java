package io.sparkadvisor.analyzer.rule;

import io.sparkadvisor.analyzer.RuleContext;
import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.finding.Severity;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * R7 — Broadcast join opportunity / fallback (heuristic).
 *
 * <p>The event log does not directly state "a broadcast was rejected", so this is a best-effort
 * heuristic on the physical plan text: if the plan uses {@code SortMergeJoin} (a shuffle join)
 * AND no {@code BroadcastHashJoin} is present, there may be an opportunity to broadcast a small
 * side. We surface this as INFO with explicit caveats — the user must confirm the small side's
 * size, since we cannot size relations from the event log alone.
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
        if (plan == null || plan.isBlank()) {
            return List.of();
        }
        boolean hasSortMergeJoin = plan.contains("SortMergeJoin");
        boolean hasBroadcastJoin = plan.contains("BroadcastHashJoin")
                || plan.contains("BroadcastNestedLoopJoin");
        if (!hasSortMergeJoin || hasBroadcastJoin) {
            return List.of();
        }

        Map<String, String> evidence = new LinkedHashMap<>();
        evidence.put("planHasSortMergeJoin", "true");
        evidence.put("planHasBroadcastJoin", "false");

        String explanation =
                "The plan uses a shuffle-based SortMergeJoin with no broadcast join present. "
                        + "If one join side is small, broadcasting it would avoid a shuffle. "
                        + "(Heuristic — confirm the small side's actual size.)";

        List<Recommendation> recs = List.of(
                Recommendation.conf(
                        "raise spark.sql.autoBroadcastJoinThreshold if the small side fits in memory",
                        "Lets Spark auto-broadcast a side under the threshold, replacing the shuffle join.",
                        "Effective only when one side is genuinely small; too high risks driver OOM."),
                Recommendation.sql(
                        "add a broadcast() hint on the small side",
                        "Forces a broadcast join regardless of the auto threshold when you know a side is small.",
                        "Explicit and reliable when the small side is known."));

        return List.of(new Finding(id(), "join", Severity.INFO, null, explanation, evidence, recs));
    }
}
