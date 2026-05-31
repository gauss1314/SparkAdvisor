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
 * R9 — Shuffle fetch wait. Triggers when reducer tasks spend a large share of task time
 * waiting for shuffle blocks, which points to remote shuffle/network/shuffle-service pressure.
 */
public final class ShuffleFetchWaitRule implements Rule {
    @Override
    public String id() {
        return "R9_SHUFFLE_FETCH_WAIT";
    }

    @Override
    public List<Finding> evaluate(RuleContext ctx) {
        List<Finding> findings = new ArrayList<Finding>();
        double warn = ctx.thresholds().shuffleFetchWaitRatioWarn();
        for (StageAnalysis st : ctx.sql().stages()) {
            if (st.shuffleReadBytes() <= 0 || st.shuffleFetchWaitMs() <= 0 || st.totalTaskTimeMs() <= 0) {
                continue;
            }
            double ratio = (double) st.shuffleFetchWaitMs() / (double) st.totalTaskTimeMs();
            if (ratio < warn) {
                continue;
            }
            Map<String, String> evidence = new LinkedHashMap<String, String>();
            evidence.put("fetchWaitMs", String.valueOf(st.shuffleFetchWaitMs()));
            evidence.put("totalTaskTimeMs", String.valueOf(st.totalTaskTimeMs()));
            evidence.put("fetchWaitRatio", String.format(java.util.Locale.ROOT, "%.2f", ratio));
            evidence.put("shuffleRemoteReadBytes", String.valueOf(st.shuffleRemoteReadBytes()));
            String explanation = String.format(java.util.Locale.ROOT,
                    "Stage %d spends %.0f%% of task time waiting on shuffle fetch; the bottleneck is likely remote shuffle/network pressure rather than CPU.",
                    st.stageId(), ratio * 100.0);
            List<Recommendation> recs = new ArrayList<Recommendation>();
            recs.add(Recommendation.sql(
                    "reduce shuffle volume before the exchange (pre-aggregate, filter earlier, or prune columns)",
                    "Lower shuffle volume reduces remote fetch pressure and reducer wait time.",
                    "Best when a large fraction of task time is fetch wait."));
            recs.add(Recommendation.conf(
                    "check shuffle service, network, executor locality, and reducer fetch tuning (spark.reducer.maxSizeInFlight, maxReqsInFlight, maxBlocksInFlightPerAddress)",
                    "High fetch wait usually points to remote shuffle pressure rather than CPU work.",
                    "Operational fix; validate with network and shuffle-service metrics."));
            findings.add(new Finding(id(), "shuffle", Severity.WARN, st.stageId(), explanation, evidence, recs));
        }
        return findings;
    }
}
