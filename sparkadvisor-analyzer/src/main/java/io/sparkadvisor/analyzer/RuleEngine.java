package io.sparkadvisor.analyzer;

import io.sparkadvisor.analyzer.rule.BroadcastJoinRule;
import io.sparkadvisor.analyzer.rule.DataSkewRule;
import io.sparkadvisor.analyzer.rule.ExcessiveSpillRule;
import io.sparkadvisor.analyzer.rule.GcPressureRule;
import io.sparkadvisor.analyzer.rule.LowParallelismRule;
import io.sparkadvisor.analyzer.rule.OverParallelismRule;
import io.sparkadvisor.analyzer.rule.Rule;
import io.sparkadvisor.analyzer.rule.SchedulingDelayRule;
import io.sparkadvisor.analyzer.rule.SmallFilesRule;
import io.sparkadvisor.core.finding.Finding;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Runs all registered {@link Rule}s over a {@link RuleContext} and returns the combined
 * findings, sorted by severity (CRITICAL first) then by rule id for stable ordering.
 *
 * <p>Stateless and reusable; rules are pure so order does not matter for correctness.
 */
public final class RuleEngine {

    private final List<Rule> rules;

    /** Engine with the default M2 rule set. */
    public RuleEngine() {
        this(defaultRules());
    }

    public RuleEngine(List<Rule> rules) {
        this.rules = List.copyOf(rules);
    }

    public static List<Rule> defaultRules() {
        return List.of(
                new DataSkewRule(),
                new ExcessiveSpillRule(),
                new LowParallelismRule(),
                new OverParallelismRule(),
                new SmallFilesRule(),
                new GcPressureRule(),
                new SchedulingDelayRule(),
                new BroadcastJoinRule());
    }

    public List<Finding> run(RuleContext ctx) {
        List<Finding> all = new ArrayList<>();
        for (Rule r : rules) {
            List<Finding> found = r.evaluate(ctx);
            if (found != null) {
                all.addAll(found);
            }
        }
        all.sort(Comparator
                .comparingInt((Finding f) -> -f.severity().ordinal()) // CRITICAL(2) first
                .thenComparing(Finding::ruleId));
        return all;
    }
}
