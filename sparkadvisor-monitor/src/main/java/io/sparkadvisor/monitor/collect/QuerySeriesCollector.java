package io.sparkadvisor.monitor.collect;

import io.sparkadvisor.analyzer.PerformanceAnalyzer;
import io.sparkadvisor.core.analyze.MetricAggregator;
import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.model.Job;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.core.util.Java8Collections;
import io.sparkadvisor.predictor.PredictionService;

import java.util.Comparator;
import java.util.Map;
import java.util.List;
import java.util.Set;

/**
 * Converts every SQL execution in an {@link ApplicationModel} into queue-level samples.
 *
 * <p>The monitor design requires all SQLs to receive lightweight metrics. Deep findings and
 * predictions are run for the slowest top-N plus a small set of representative strata
 * (spill/fetch/GC/skew/template), limiting work without making the queue summary purely top-N.
 */
public final class QuerySeriesCollector {

    private final int topN;
    private final int samplePerStratum;
    private final MetricAggregator aggregator;
    private final PerformanceAnalyzer performanceAnalyzer = new PerformanceAnalyzer();
    private final PredictionService predictionService = new PredictionService();

    public QuerySeriesCollector(ApplicationModel app, int topN) {
        this(app, topN, 5);
    }

    public QuerySeriesCollector(ApplicationModel app, int topN, int samplePerStratum) {
        this.topN = Math.max(1, topN);
        this.samplePerStratum = Math.max(0, samplePerStratum);
        this.aggregator = new MetricAggregator(app);
    }

    public List<QuerySample> collect(ApplicationModel app) {
        List<LightSample> light = app.sqlExecutions().stream()
                .sorted(Comparator.comparingLong(SqlExecution::startTime))
                .map(sql -> lightSample(app, sql))
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
        List<QuerySample> lightSamples = light.stream()
                .map(sample -> sample.toQuerySample(false, performanceAnalyzer, predictionService, app.conf()))
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
        Set<Long> deepExecutionIds = new DeepAnalysisSelector(topN, samplePerStratum).select(lightSamples);

        return Java8Collections.listCopy(light.stream()
                .map(sample -> sample.toQuerySample(deepExecutionIds.contains(sample.sql.executionId()),
                        performanceAnalyzer, predictionService, app.conf()))
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new)));
    }

    private LightSample lightSample(ApplicationModel app, SqlExecution sql) {
        SqlAnalysis analysis = aggregator.analyze(sql);
        long shuffleRead = analysis.stages().stream().mapToLong(StageAnalysis::shuffleReadBytes).sum();
        long shuffleWrite = analysis.stages().stream().mapToLong(StageAnalysis::shuffleWriteBytes).sum();
        long spill = analysis.stages().stream().mapToLong(StageAnalysis::spillBytes).sum();
        long input = analysis.stages().stream().mapToLong(StageAnalysis::inputBytes).sum();
        long totalTaskTime = analysis.stages().stream().mapToLong(StageAnalysis::totalTaskTimeMs).sum();
        long fetchWait = analysis.stages().stream().mapToLong(StageAnalysis::shuffleFetchWaitMs).sum();
        int failedAttempts = analysis.stages().stream().mapToInt(StageAnalysis::failedTaskAttempts).sum();
        int extraAttempts = analysis.stages().stream().mapToInt(StageAnalysis::extraTaskAttempts).sum();
        double maxSkew = analysis.stages().stream()
                .mapToDouble(StageAnalysis::skewRatio)
                .max()
                .orElse(0.0);
        double maxGc = analysis.stages().stream()
                .mapToDouble(StageAnalysis::gcRatio)
                .max()
                .orElse(0.0);
        double fetchWaitRatio = totalTaskTime <= 0L ? 0.0 : (double) fetchWait / (double) totalTaskTime;
        return new LightSample(sql, analysis, failed(app, sql), shuffleRead, shuffleWrite, spill, input,
                totalTaskTime, fetchWait, failedAttempts, extraAttempts, maxSkew, maxGc,
                analysis.coreUtilization(), fetchWaitRatio, templateHash(sql.description()));
    }

    private static String templateHash(String sql) {
        String normalized = sql == null ? "" : sql
                .replaceAll("/\\*.*?\\*/", " ")
                .replaceAll("'[^']*'", "?")
                .replaceAll("\\b\\d+\\b", "?")
                .replaceAll("\\s+", " ")
                .trim()
                .toLowerCase(java.util.Locale.ROOT);
        return Integer.toHexString(normalized.hashCode());
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

    private static final class LightSample {
        private final SqlExecution sql;
        private final SqlAnalysis analysis;
        private final boolean failed;
        private final long shuffleRead;
        private final long shuffleWrite;
        private final long spill;
        private final long input;
        private final long totalTaskTime;
        private final long fetchWait;
        private final int failedAttempts;
        private final int extraAttempts;
        private final double maxSkew;
        private final double maxGc;
        private final double coreUtilization;
        private final double fetchWaitRatio;
        private final String templateHash;

        LightSample(SqlExecution sql, SqlAnalysis analysis, boolean failed, long shuffleRead,
                    long shuffleWrite, long spill, long input, long totalTaskTime, long fetchWait,
                    int failedAttempts, int extraAttempts, double maxSkew, double maxGc,
                    double coreUtilization, double fetchWaitRatio, String templateHash) {
            this.sql = sql; this.analysis = analysis; this.failed = failed; this.shuffleRead = shuffleRead;
            this.shuffleWrite = shuffleWrite; this.spill = spill; this.input = input;
            this.totalTaskTime = totalTaskTime; this.fetchWait = fetchWait;
            this.failedAttempts = failedAttempts; this.extraAttempts = extraAttempts;
            this.maxSkew = maxSkew; this.maxGc = maxGc; this.coreUtilization = coreUtilization;
            this.fetchWaitRatio = fetchWaitRatio; this.templateHash = templateHash;
        }

        QuerySample toQuerySample(boolean deep, PerformanceAnalyzer analyzer,
                                  PredictionService predictionService, Map<String, String> conf) {
            List<Finding> findings = deep
                    ? analyzer.analyze(analysis, conf)
                    : Java8Collections.<Finding>listOf();
            PredictionService.Predictions predictions = deep
                    ? predictionService.predict(analysis, conf)
                    : null;
            return new QuerySample(
                    sql.executionId(), sql.statementId(), sql.description(), templateHash,
                    sql.startTime(), sql.endTime(), sql.incomplete(), failed, sql.wallClockMs(),
                    analysis.stages().size(), shuffleRead, shuffleWrite, spill, input,
                    totalTaskTime, fetchWait, failedAttempts, extraAttempts, maxSkew, maxGc,
                    coreUtilization, fetchWaitRatio, analysis, findings, predictions);
        }
    }
}
