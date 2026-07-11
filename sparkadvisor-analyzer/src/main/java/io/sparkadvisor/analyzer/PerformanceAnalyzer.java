package io.sparkadvisor.analyzer;

import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.analyzer.v2.MetricsContext;
import io.sparkadvisor.analyzer.v2.RuleEngineV2;
import io.sparkadvisor.analyzer.v2.RuleThresholdsV2;
import io.sparkadvisor.analyzer.v2.SqlMetricsContextAdapter;
import io.sparkadvisor.analyzer.v2.RuleRunResult;

import java.util.List;
import java.util.Map;

/**
 * Convenience facade: turns a {@link SqlAnalysis} plus the application's spark conf into a
 * list of {@link Finding}s. This is what the report/CLI layer calls.
 */
public final class PerformanceAnalyzer {

    private final RuleThresholds thresholds;
    private final RuleEngine engine;
    private final RuleEngineV2 engineV2;
    private final RuleThresholdsV2 configuredV2Thresholds;

    public PerformanceAnalyzer() {
        this(RuleThresholds.defaults(), new RuleEngine());
    }

    public PerformanceAnalyzer(RuleThresholds thresholds, RuleEngine engine) {
        this(thresholds, engine, null);
    }

    public PerformanceAnalyzer(RuleThresholdsV2 thresholds) {
        this(RuleThresholds.defaults(), new RuleEngine(), thresholds);
    }

    private PerformanceAnalyzer(RuleThresholds thresholds, RuleEngine engine, RuleThresholdsV2 configuredV2Thresholds) {
        this.thresholds = thresholds;
        this.engine = engine;
        this.configuredV2Thresholds = configuredV2Thresholds;
        this.engineV2 = RuleEngineV2.sqlDefaults(configuredV2Thresholds == null ? RuleThresholdsV2.defaults() : configuredV2Thresholds);
    }

    /**
     * @param sql  the analyzed SQL (from core's MetricAggregator)
     * @param conf the application's spark.* configuration (for AQE awareness)
     */
    public List<Finding> analyze(SqlAnalysis sql, Map<String, String> conf) {
        AqeContext aqe = AqeContext.from(conf);
        return engine.run(new RuleContext(sql, thresholds, aqe));
    }

    /** Stable S/DQ rule ids defined by docs/rules.md. */
    public List<Finding> analyzeV2(SqlAnalysis sql, Map<String, String> conf) {
        return analyzeV2(sql, conf, false);
    }

    public List<Finding> analyzeV2(SqlAnalysis sql, Map<String, String> conf, boolean incomplete) {
        return analyzeV2Detailed(sql, conf, incomplete).findings();
    }

    public RuleRunResult analyzeV2Detailed(SqlAnalysis sql, Map<String, String> conf, boolean incomplete) {
        RuleThresholdsV2 active = configuredV2Thresholds == null ? RuleThresholdsV2.from(conf) : configuredV2Thresholds;
        return RuleEngineV2.sqlDefaults(active)
                .evaluateDetailed(SqlMetricsContextAdapter.from(sql, conf, incomplete));
    }

    /** Evaluate a pre-aggregated context, including optional plan/baseline/quality capabilities. */
    public List<Finding> analyze(MetricsContext context) {
        return engineV2.evaluate(java.util.Collections.singletonList(context));
    }

    /** Compatibility entry point for callers that still need pre-v2 R ids during migration. */
    public List<Finding> analyzeLegacy(SqlAnalysis sql, Map<String, String> conf) {
        return analyze(sql, conf);
    }
}
