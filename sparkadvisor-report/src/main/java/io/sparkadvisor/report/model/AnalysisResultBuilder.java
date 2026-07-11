package io.sparkadvisor.report.model;

import io.sparkadvisor.analyzer.PerformanceAnalyzer;
import io.sparkadvisor.core.analyze.MetricAggregator;
import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.predictor.PredictionService;
import io.sparkadvisor.analyzer.v2.RuleThresholdsV2;
import io.sparkadvisor.analyzer.v2.RuleRunResult;
import io.sparkadvisor.analyzer.v2.Capability;

import java.time.Instant;
import java.util.List;

/**
 * Assembles an {@link AnalysisResult} from a parsed {@link ApplicationModel} and a target
 * {@link SqlExecution}: aggregates hard metrics (core), runs the rule engine (analyzer) to
 * populate findings, and runs the predictors (predictor). The LLM advice block (F4) is null.
 */
public final class AnalysisResultBuilder {

    public static final String VERSION = "0.1.0";

    private final ApplicationModel app;
    private final MetricAggregator aggregator;
    private final PerformanceAnalyzer performanceAnalyzer;
    private final PredictionService predictionService;
    private final String sourcePath;

    public AnalysisResultBuilder(ApplicationModel app, String sourcePath) {
        this(app, sourcePath, null);
    }

    public AnalysisResultBuilder(ApplicationModel app, String sourcePath, RuleThresholdsV2 thresholds) {
        this.app = app;
        this.aggregator = new MetricAggregator(app);
        this.performanceAnalyzer = thresholds == null ? new PerformanceAnalyzer() : new PerformanceAnalyzer(thresholds);
        this.predictionService = new PredictionService();
        this.sourcePath = sourcePath;
    }

    public AnalysisResult build(SqlExecution target) {
        SqlAnalysis sqlAnalysis = (target == null) ? null : aggregator.analyze(target);
        RuleRunResult ruleRun = (sqlAnalysis == null) ? null
                : performanceAnalyzer.analyzeV2Detailed(sqlAnalysis, app.conf(), app.incomplete());
        List<Finding> findings = ruleRun == null ? new java.util.ArrayList<Finding>() : ruleRun.findings();
        PredictionService.Predictions predictions = (sqlAnalysis == null)
                ? new PredictionService.Predictions(null, null)
                : predictionService.predict(sqlAnalysis, app.conf());
        return new AnalysisResult(
                appSummary(),
                sqlAnalysis,
                findings,
                predictions.shuffle(),
                predictions.executor(),
                null,                      // aiAdvice: F4
                meta(ruleRun));
    }

    private AnalysisResult.AppSummary appSummary() {
        return new AnalysisResult.AppSummary(
                app.appId(),
                app.appName(),
                app.wallClockMs(),
                app.sqlExecutions().size(),
                app.jobs().size(),
                app.stages().size(),
                readCores());
    }

    private AnalysisResult.Meta meta(RuleRunResult ruleRun) {
        java.util.Map<String,java.util.List<String>> unavailable = new java.util.LinkedHashMap<String,java.util.List<String>>();
        if (ruleRun != null) {
            for (java.util.Map.Entry<String,java.util.Set<Capability>> entry : ruleRun.unavailableRules().entrySet()) {
                java.util.List<String> names = new java.util.ArrayList<String>();
                for (Capability capability : entry.getValue()) names.add(capability.name());
                unavailable.put(entry.getKey(), names);
            }
        }
        return new AnalysisResult.Meta(
                VERSION,
                Instant.now().toString(),
                app.incomplete(),
                sourcePath,
                unavailable);
    }

    private int readCores() {
        int instances = parse(app.conf().get("spark.executor.instances"), 0);
        int cores = parse(app.conf().get("spark.executor.cores"), 1);
        int total = instances * cores;
        return total > 0 ? total : 1;
    }

    private static int parse(String v, int dflt) {
        if (v == null) return dflt;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return dflt;
        }
    }
}
