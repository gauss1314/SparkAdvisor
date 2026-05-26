package io.sparkadvisor.report.model;

import io.sparkadvisor.analyzer.PerformanceAnalyzer;
import io.sparkadvisor.core.analyze.MetricAggregator;
import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.predictor.PredictionService;

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
        this.app = app;
        this.aggregator = new MetricAggregator(app);
        this.performanceAnalyzer = new PerformanceAnalyzer();
        this.predictionService = new PredictionService();
        this.sourcePath = sourcePath;
    }

    public AnalysisResult build(SqlExecution target) {
        SqlAnalysis sqlAnalysis = (target == null) ? null : aggregator.analyze(target);
        List<Finding> findings = (sqlAnalysis == null)
                ? new java.util.ArrayList<>()
                : performanceAnalyzer.analyze(sqlAnalysis, app.conf());
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
                meta());
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

    private AnalysisResult.Meta meta() {
        return new AnalysisResult.Meta(
                VERSION,
                Instant.now().toString(),
                app.incomplete(),
                sourcePath);
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
