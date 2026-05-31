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
 * R10 — Task attempts. Runtime ID remains R10_TASK_RETRY for JSON compatibility. Failed
 * attempts and extra/speculative attempts are separate signals: failures usually point to
 * instability, while extra attempts may be normal speculation when spark.speculation is enabled.
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
            evidence.put("failedAndSpeculativeAreSeparated", "true");
            evidence.put("extraAttemptsMayBeSpeculation", String.valueOf(extraTrips && !failedTrips));
            String explanation = String.format(java.util.Locale.ROOT,
                    "Stage %d has %d failed task attempt(s) and %d extra/speculative attempt(s); "
                            + "failures and speculation both add wall-clock noise but require different follow-up.",
                    st.stageId(), st.failedTaskAttempts(), st.extraTaskAttempts());
            List<Recommendation> recs = new ArrayList<Recommendation>();
            if (failedTrips) {
                recs.add(Recommendation.conf(
                        "inspect failed task logs, TaskEndReason, and executor/container health for this stage",
                        "Failed retries add wall-clock noise and can hide the real bottleneck behind instability.",
                        "Required before trusting fine-grained tuning recommendations."));
            } else {
                recs.add(Recommendation.conf(
                        "check spark.speculation and slow-task evidence before treating extra attempts as failures",
                        "Speculative duplicate attempts can be healthy compensation for stragglers, not necessarily instability.",
                        "Use TaskEndReason and speculation settings to classify the attempts."));
            }
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
