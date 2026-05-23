package io.sparkadvisor.analyzer.rule;

import io.sparkadvisor.analyzer.RuleContext;
import io.sparkadvisor.core.finding.Finding;

import java.util.List;

/**
 * A single performance rule. Given the analyzed SQL plus thresholds and AQE context,
 * it returns zero or more {@link Finding}s.
 *
 * <p>Rules are stateless and side-effect free, so the engine can run them in any order.
 */
public interface Rule {

    /** Stable identifier, e.g. "R1_DATA_SKEW". Used in findings and tests. */
    String id();

    /** Evaluate and return findings (possibly empty, never null). */
    List<Finding> evaluate(RuleContext ctx);
}
