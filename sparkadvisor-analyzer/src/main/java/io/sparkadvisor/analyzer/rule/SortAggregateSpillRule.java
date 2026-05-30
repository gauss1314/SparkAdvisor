package io.sparkadvisor.analyzer.rule;

import io.sparkadvisor.analyzer.RuleContext;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.finding.Severity;
import io.sparkadvisor.core.util.Strings;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * R11 — Physical-plan attribution for spill. Event logs only expose spill at task/stage level;
 * this heuristic connects that spill to sort/aggregate-heavy plans when the plan text supports it.
 */
public final class SortAggregateSpillRule implements Rule {
    @Override
    public String id() {
        return "R11_SORT_AGG_SPILL";
    }

    @Override
    public List<Finding> evaluate(RuleContext ctx) {
        List<Finding> findings = new ArrayList<Finding>();
        String plan = ctx.sql().physicalPlanText();
        if (Strings.isBlank(plan)) {
            return findings;
        }
        boolean hasSort = plan.contains("Sort");
        boolean hasAggregate = plan.contains("HashAggregate")
                || plan.contains("ObjectHashAggregate")
                || plan.contains("SortAggregate");
        if (!hasSort && !hasAggregate) {
            return findings;
        }
        long totalSpill = ctx.sql().stages().stream().mapToLong(s -> s.spillBytes()).sum();
        if (totalSpill <= 0L) {
            return findings;
        }
        Map<String, String> evidence = new LinkedHashMap<String, String>();
        evidence.put("planHasSort", String.valueOf(hasSort));
        evidence.put("planHasAggregate", String.valueOf(hasAggregate));
        evidence.put("totalSpillBytes", String.valueOf(totalSpill));
        String explanation = "The physical plan contains sort/aggregate operators and the SQL spills; "
                + "memory pressure is likely concentrated around those operators.";
        List<Recommendation> recs = new ArrayList<Recommendation>();
        recs.add(Recommendation.sql(
                "reduce sort/aggregate working set before the spilling operator",
                "The physical plan contains sort or aggregate operators and the SQL spills; reducing rows/columns before those operators lowers memory pressure.",
                "Usually more reliable than only increasing memory."));
        recs.add(Recommendation.conf(
                "raise memory headroom or use smaller shuffle/advisory partitions for the spilling stage",
                "Smaller operator inputs are less likely to spill during sort/aggregate.",
                "Trade-off: more tasks and scheduling overhead."));
        findings.add(new Finding(id(), "plan", Severity.INFO, null, explanation, evidence, recs));
        return findings;
    }
}
