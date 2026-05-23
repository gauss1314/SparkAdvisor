package io.sparkadvisor.analyzer;

import io.sparkadvisor.core.analyze.SqlAnalysis;

/**
 * Everything a rule needs to evaluate one SQL: the aggregated analysis, the thresholds,
 * and the AQE configuration context.
 */
public record RuleContext(
        SqlAnalysis sql,
        RuleThresholds thresholds,
        AqeContext aqe) {
}
