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
 * R3 — Low parallelism / under-utilization. Triggers when core utilization across the SQL's
 * stages is below the threshold, meaning slots sit idle (too few partitions, or scheduling
 * wait). Reported at SQL level (targetStageId = null).
 */
public final class LowParallelismRule implements Rule {

    @Override
    public String id() {
        return "R3_LOW_PARALLELISM";
    }

    @Override
    public List<Finding> evaluate(RuleContext ctx) {
        SqlAnalysis sql = ctx.sql();
        double util = sql.coreUtilization();
        if (util >= ctx.thresholds().coreUtilLow() || util <= 0) {
            return List.of();
        }
        Map<String, String> evidence = new LinkedHashMap<>();
        evidence.put("coreUtilization", String.format("%.2f", util));
        evidence.put("criticalPathMs", String.valueOf(sql.criticalPathMs()));
        evidence.put("idealMs", String.valueOf(sql.idealMs()));

        String explanation = String.format(
                "Core utilization is %.0f%%; executor slots are largely idle, suggesting too "
                        + "few partitions or scheduling/resource wait.", util * 100);

        List<Recommendation> recs = new java.util.ArrayList<>();
        if (ctx.aqe().aqeEnabled() && ctx.aqe().coalesceEnabled()) {
            recs.add(Recommendation.conf(
                    "lower spark.sql.adaptive.advisoryPartitionSizeInBytes "
                            + "(or raise coalescePartitions.initialPartitionNum)",
                    "AQE may be coalescing into too few partitions; smaller advisory size keeps "
                            + "more partitions so more cores stay busy.",
                    "Raises parallelism without a query rewrite."));
        } else {
            recs.add(Recommendation.conf(
                    "increase spark.sql.shuffle.partitions (or repartition the input)",
                    "More partitions spread work across more cores.",
                    "Effective when tasks are large and few."));
        }
        return List.of(new Finding(id(), "parallelism", Severity.WARN, null,
                explanation, evidence, recs));
    }
}
