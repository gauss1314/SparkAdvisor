package io.sparkadvisor.monitor.collect;

import io.sparkadvisor.analyzer.PerformanceAnalyzer;
import io.sparkadvisor.core.analyze.MetricAggregator;
import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.model.Job;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.predictor.PredictionService;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Converts every SQL execution in an {@link ApplicationModel} into queue-level samples.
 *
 * <p>The monitor design requires top-N deep analysis: all SQLs receive lightweight metrics,
 * while only the slowest completed top-N receive findings and predictions.
 */
public final class QuerySeriesCollector {

    private final int topN;
    private final MetricAggregator aggregator;
    private final PerformanceAnalyzer performanceAnalyzer = new PerformanceAnalyzer();
    private final PredictionService predictionService = new PredictionService();

    public QuerySeriesCollector(ApplicationModel app, int topN) {
        this.topN = Math.max(1, topN);
        this.aggregator = new MetricAggregator(app);
    }

    public List<QuerySample> collect(ApplicationModel app) {
        Set<Long> deepExecutionIds = app.sqlExecutions().stream()
                .filter(sql -> !sql.incomplete() && sql.wallClockMs() > 0L)
                .sorted(Comparator.comparingLong(SqlExecution::wallClockMs).reversed())
                .limit(topN)
                .map(SqlExecution::executionId)
                .collect(java.util.stream.Collectors.toCollection(HashSet::new));

        return app.sqlExecutions().stream()
                .sorted(Comparator.comparingLong(SqlExecution::startTime))
                .map(sql -> sample(app, sql, deepExecutionIds.contains(sql.executionId())))
                .toList();
    }

    private QuerySample sample(ApplicationModel app, SqlExecution sql, boolean deep) {
        SqlAnalysis analysis = aggregator.analyze(sql);
        long shuffleRead = analysis.stages().stream().mapToLong(StageAnalysis::shuffleReadBytes).sum();
        long shuffleWrite = analysis.stages().stream().mapToLong(StageAnalysis::shuffleWriteBytes).sum();
        long spill = analysis.stages().stream().mapToLong(StageAnalysis::spillBytes).sum();
        double maxSkew = analysis.stages().stream()
                .mapToDouble(StageAnalysis::skewRatio)
                .max()
                .orElse(0.0);
        double maxGc = analysis.stages().stream()
                .mapToDouble(StageAnalysis::gcRatio)
                .max()
                .orElse(0.0);

        List<Finding> findings = deep
                ? performanceAnalyzer.analyze(analysis, app.conf())
                : List.of();
        PredictionService.Predictions predictions = deep
                ? predictionService.predict(analysis, app.conf())
                : null;

        return new QuerySample(
                sql.executionId(),
                sql.statementId(),
                sql.description(),
                sql.startTime(),
                sql.endTime(),
                sql.incomplete(),
                failed(app, sql),
                sql.wallClockMs(),
                analysis.stages().size(),
                shuffleRead,
                shuffleWrite,
                spill,
                maxSkew,
                maxGc,
                analysis.coreUtilization(),
                analysis,
                findings,
                predictions);
    }

    private boolean failed(ApplicationModel app, SqlExecution sql) {
        return app.jobs().stream()
                .filter(job -> belongsTo(sql, job))
                .anyMatch(Job::failed);
    }

    private boolean belongsTo(SqlExecution sql, Job job) {
        return (job.sqlExecutionId() != null && job.sqlExecutionId() == sql.executionId())
                || sql.jobIds().contains((long) job.jobId());
    }
}
