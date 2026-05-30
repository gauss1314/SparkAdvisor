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
 * R10 — Task retries / failed attempts. Retries inflate wall clock and reduce confidence in
 * fine-grained tuning conclusions until the underlying instability is understood.
 */
public final class TaskRetryRule implements Rule {
    @Override
    public String id() {
        return "R10_TASK_RETRY";
    }

    @Override
    public List<Finding> evaluate(RuleContext ctx) {
        List<Finding> findings = new ArrayList<Finding>();
        for (StageAnalysis st : ctx.sql().stages()) {
            double extraRatio = st.numTasks() <= 0 ? 0.0
                    : (double) st.extraTaskAttempts() / (double) st.numTasks();
            boolean failedTrips = st.failedTaskAttempts() >= ctx.thresholds().failedTaskAttemptsWarn();
            boolean extraTrips = extraRatio >= ctx.thresholds().extraTaskAttemptRatioWarn();
            if (!failedTrips && !extraTrips) {
                continue;
            }
            Map<String, String> evidence = new LinkedHashMap<String, String>();
            evidence.put("failedTaskAttempts", String.valueOf(st.failedTaskAttempts()));
            evidence.put("extraTaskAttempts", String.valueOf(st.extraTaskAttempts()));
            evidence.put("extraTaskAttemptRatio", String.format(java.util.Locale.ROOT, "%.2f", extraRatio));
            String explanation = String.format(java.util.Locale.ROOT,
                    "Stage %d has %d failed task attempt(s) and %d extra attempt(s); retries can inflate wall clock and obscure the root cause.",
                    st.stageId(), st.failedTaskAttempts(), st.extraTaskAttempts());
            List<Recommendation> recs = new ArrayList<Recommendation>();
            recs.add(Recommendation.conf(
                    "inspect failed task logs and executor/container health for this stage",
                    "Retries add wall-clock noise and can hide the real bottleneck behind failed attempts.",
                    "Required before trusting fine-grained tuning recommendations."));
            recs.add(Recommendation.conf(
                    "reduce per-task input/shuffle size if failures are memory or timeout related",
                    "Smaller tasks reduce memory pressure and timeout blast radius.",
                    "Helps when failures correlate with large partitions."));
            findings.add(new Finding(id(), "reliability", failedTrips ? Severity.WARN : Severity.INFO,
                    st.stageId(), explanation, evidence, recs));
        }
        return findings;
    }
}
