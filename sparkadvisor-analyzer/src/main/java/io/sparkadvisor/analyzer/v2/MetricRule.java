package io.sparkadvisor.analyzer.v2;

import io.sparkadvisor.core.finding.Finding;

import java.util.List;
import java.util.Set;

public interface MetricRule {
    String id();
    RuleScope scope();
    Set<Capability> requires();
    Set<String> thresholdKeys();
    List<Finding> evaluate(MetricsContext context, RuleThresholdsV2 thresholds);
}
