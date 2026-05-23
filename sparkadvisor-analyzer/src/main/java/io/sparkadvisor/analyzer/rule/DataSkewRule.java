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
 * R1 — Data skew. Triggers when a stage's task-duration max/median (or shuffle-read
 * max/median) exceeds the threshold.
 *
 * <p>AQE-aware: the recommendation depends on whether AQE skew-join handling is already on.
 * If it is and skew persists, we suggest tuning the skew factor/threshold rather than the
 * useless "enable AQE". If it's off, enabling it is the first, cheapest fix.
 */
public final class DataSkewRule implements Rule {

    @Override
    public String id() {
        return "R1_DATA_SKEW";
    }

    @Override
    public List<Finding> evaluate(RuleContext ctx) {
        RuleThresholds t = ctx.thresholds();
        List<Finding> findings = new ArrayList<>();
        for (StageAnalysis st : ctx.sql().stages()) {
            double durSkew = st.skewRatio();
            double shufSkew = st.shuffleSkewRatio();
            boolean durTrips = durSkew >= t.skewRatioWarn();
            boolean shufTrips = shufSkew >= t.shuffleSkewWarn();
            if (!durTrips && !shufTrips) {
                continue;
            }
            Severity sev = durSkew >= t.skewRatioCritical() ? Severity.CRITICAL : Severity.WARN;

            Map<String, String> evidence = new LinkedHashMap<>();
            evidence.put("durationSkewRatio", String.format("%.1f", durSkew));
            evidence.put("shuffleReadSkewRatio", String.format("%.1f", shufSkew));
            evidence.put("maxTaskMs", String.valueOf(st.maxTaskMs()));
            evidence.put("medianTaskMs", String.valueOf(st.medianTaskMs()));

            String explanation = String.format(
                    "Stage %d is skewed: the slowest task is %.1f× the median "
                            + "(adding executors will not help this stage).",
                    st.stageId(), Math.max(durSkew, shufSkew));

            findings.add(new Finding(
                    id(), "skew", sev, st.stageId(), explanation, evidence,
                    recommendations(ctx)));
        }
        return findings;
    }

    private List<Recommendation> recommendations(RuleContext ctx) {
        List<Recommendation> recs = new ArrayList<>();
        var aqe = ctx.aqe();
        if (!aqe.aqeEnabled()) {
            recs.add(Recommendation.conf(
                    "set spark.sql.adaptive.enabled=true; set spark.sql.adaptive.skewJoin.enabled=true",
                    "AQE is disabled; enabling adaptive skew-join handling lets Spark split "
                            + "skewed partitions automatically at runtime.",
                    "Often the single most effective fix for join skew."));
        } else if (!aqe.skewJoinEnabled()) {
            recs.add(Recommendation.conf(
                    "set spark.sql.adaptive.skewJoin.enabled=true",
                    "AQE is on but skew-join handling is off; turning it on lets Spark split "
                            + "skewed join partitions.",
                    "Targets join skew specifically."));
        } else {
            // AQE skew join already on but skew persists -> tune the factor/threshold.
            recs.add(Recommendation.conf(
                    "lower spark.sql.adaptive.skewJoin.skewedPartitionFactor "
                            + "and/or skewedPartitionThresholdInBytes",
                    "AQE skew-join is already enabled but skew remains; making the detector "
                            + "more aggressive can catch partitions it currently misses.",
                    "Incremental; effectiveness depends on the actual key distribution."));
            recs.add(Recommendation.sql(
                    "salt the skewed join/group key (add a random prefix, then aggregate in two passes)",
                    "When a single key dominates, AQE's per-partition split has limited room; "
                            + "salting spreads that key across tasks.",
                    "Most reliable for a known single hot key; requires query rewrite."));
        }
        return recs;
    }
}
