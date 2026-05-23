package io.sparkadvisor.monitor.rule;

/**
 * Centralized thresholds for queue-level rules. These are deliberately separate from the
 * single-SQL {@code RuleThresholds}: queue recommendations require statistical support across
 * many queries, not just one bad stage.
 */
public record QueueRuleThresholds(
        int minAnalyzedQueries,
        double commonBottleneckPct,
        double mixedPartitionPct,
        double highUtilization,
        double lowUtilization,
        double contentionLimitedPct) {

    public static QueueRuleThresholds defaults() {
        return new QueueRuleThresholds(
                5,
                0.30,
                0.15,
                0.85,
                0.35,
                0.25);
    }
}
