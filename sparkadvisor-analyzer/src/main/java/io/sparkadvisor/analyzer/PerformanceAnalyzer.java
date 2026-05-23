package io.sparkadvisor.analyzer;

import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.finding.Finding;

import java.util.List;
import java.util.Map;

/**
 * Convenience facade: turns a {@link SqlAnalysis} plus the application's spark conf into a
 * list of {@link Finding}s. This is what the report/CLI layer calls.
 */
public final class PerformanceAnalyzer {

    private final RuleThresholds thresholds;
    private final RuleEngine engine;

    public PerformanceAnalyzer() {
        this(RuleThresholds.defaults(), new RuleEngine());
    }

    public PerformanceAnalyzer(RuleThresholds thresholds, RuleEngine engine) {
        this.thresholds = thresholds;
        this.engine = engine;
    }

    /**
     * @param sql  the analyzed SQL (from core's MetricAggregator)
     * @param conf the application's spark.* configuration (for AQE awareness)
     */
    public List<Finding> analyze(SqlAnalysis sql, Map<String, String> conf) {
        AqeContext aqe = AqeContext.from(conf);
        RuleContext ctx = new RuleContext(sql, thresholds, aqe);
        return engine.run(ctx);
    }
}
