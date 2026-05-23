package io.sparkadvisor.core.finding;

import java.util.List;
import java.util.Map;

/**
 * One detected issue produced by the rule engine (analyzer, M2). The report renders these
 * sorted by severity. In M1 the list is typically empty; the contract and rendering exist
 * so the analyzer can populate it without touching the report.
 *
 * @param ruleId       stable rule identifier, e.g. "R1_DATA_SKEW"
 * @param category     grouping label, e.g. "skew", "spill", "parallelism"
 * @param severity     INFO / WARN / CRITICAL
 * @param targetStageId the stage this finding concerns, or null if app-level
 * @param explanation  human-readable description of the problem
 * @param evidence     metric name -&gt; value used to justify the finding
 * @param recommendations suggested actions
 */
public record Finding(
        String ruleId,
        String category,
        Severity severity,
        Integer targetStageId,
        String explanation,
        Map<String, String> evidence,
        List<Recommendation> recommendations) {
}
