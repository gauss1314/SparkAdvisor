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
 * R6 — GC pressure. Triggers when a stage spends more than the threshold fraction of task
 * time in JVM garbage collection.
 */
public final class GcPressureRule implements Rule {

    @Override
    public String id() {
        return "R6_GC_PRESSURE";
    }

    @Override
    public List<Finding> evaluate(RuleContext ctx) {
        List<Finding> findings = new ArrayList<>();
        double warn = ctx.thresholds().gcRatioWarn();
        for (StageAnalysis st : ctx.sql().stages()) {
            if (st.gcRatio() < warn) {
                continue;
            }
            Map<String, String> evidence = new LinkedHashMap<>();
            evidence.put("gcRatio", String.format("%.2f", st.gcRatio()));

            String explanation = String.format(
                    "Stage %d spends %.0f%% of task time in GC, indicating memory pressure.",
                    st.stageId(), st.gcRatio() * 100);

            List<Recommendation> recs = new java.util.ArrayList<Recommendation>(java.util.Arrays.asList(
                    Recommendation.sql(
                            "review object-heavy UDFs, non-vectorized readers, object aggregation, and cache layout",
                            "High GC is often caused by allocation churn and Java object expansion before it is a pure heap-size problem.",
                            "Best root-cause fix when UDFs or row/object paths are present."),
                    Recommendation.conf(
                            "increase executor memory or reduce per-task data "
                                    + "(more partitions / smaller advisory size)",
                            "Less live data per task lowers allocation churn and GC time.",
                            "Pairs well with fixing any spill on the same stage."),
                    Recommendation.conf(
                            "consider the G1 collector and review object-heavy UDFs",
                            "G1 handles large heaps better; heavy intermediate objects drive GC.",
                            "Workload-dependent.")));

            findings.add(new Finding(id(), "gc", Severity.WARN, st.stageId(),
                    explanation, evidence, recs));
        }
        return findings;
    }
}
