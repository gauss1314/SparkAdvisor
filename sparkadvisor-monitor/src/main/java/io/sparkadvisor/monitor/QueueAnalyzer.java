package io.sparkadvisor.monitor;

import io.sparkadvisor.core.EventLogAnalyzer;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.monitor.aggregate.QueueAggregator;
import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;
import io.sparkadvisor.monitor.collect.QuerySample;
import io.sparkadvisor.monitor.collect.QuerySeriesCollector;
import io.sparkadvisor.monitor.contention.ContentionTimeline;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

/**
 * Monitor-module facade: event-log path to queue-level analysis result.
 */
public final class QueueAnalyzer {

    public static final int DEFAULT_TOP_N = 50;
    public static final int DEFAULT_SAMPLE_PER_STRATUM = 5;
    public static final long DEFAULT_BUCKET_MS = 60L * 60L * 1000L;

    private final EventLogAnalyzer eventLogAnalyzer;

    public QueueAnalyzer() {
        this(new Configuration());
    }

    public QueueAnalyzer(Configuration hadoopConf) {
        this.eventLogAnalyzer = new EventLogAnalyzer(hadoopConf);
    }

    public QueueAnalysisResult analyze(String path) throws IOException {
        return analyze(path, DEFAULT_TOP_N, DEFAULT_BUCKET_MS);
    }

    public QueueAnalysisResult analyze(String path, int topN, long bucketMs) throws IOException {
        return analyze(path, topN, DEFAULT_SAMPLE_PER_STRATUM, bucketMs);
    }

    public QueueAnalysisResult analyze(String path, int topN, int samplePerStratum, long bucketMs) throws IOException {
        return analyze(path, topN, samplePerStratum, bucketMs, QueueAnalysisContext.defaults());
    }

    public QueueAnalysisResult analyze(String path, int topN, int samplePerStratum, long bucketMs,
                                       QueueAnalysisContext context) throws IOException {
        ApplicationModel app = eventLogAnalyzer.analyze(path, true);
        return analyze(app, path, topN, samplePerStratum, bucketMs, context);
    }

    public QueueAnalysisResult analyze(ApplicationModel app, String sourcePath, int topN, long bucketMs) {
        return analyze(app, sourcePath, topN, DEFAULT_SAMPLE_PER_STRATUM, bucketMs);
    }

    public QueueAnalysisResult analyze(ApplicationModel app, String sourcePath, int topN,
                                       int samplePerStratum, long bucketMs) {
        return analyze(app, sourcePath, topN, samplePerStratum, bucketMs, QueueAnalysisContext.defaults());
    }

    public QueueAnalysisResult analyze(ApplicationModel app, String sourcePath, int topN,
                                       int samplePerStratum, long bucketMs, QueueAnalysisContext context) {
        int normalizedTopN = Math.max(1, topN);
        int normalizedSamplePerStratum = Math.max(0, samplePerStratum);
        long normalizedBucketMs = Math.max(60_000L, bucketMs);
        List<QuerySample> samples = new QuerySeriesCollector(app, normalizedTopN, normalizedSamplePerStratum).collect(app);
        long windowStart = samples.stream()
                .mapToLong(s -> s.startTime() > 0L ? s.startTime() : app.startTime())
                .filter(v -> v > 0L)
                .min()
                .orElse(app.startTime());
        long windowEnd = samples.stream()
                .mapToLong(s -> Math.max(s.endTime(), s.startTime()))
                .max()
                .orElse(app.endTime());
        if (app.endTime() > 0L) {
            windowEnd = Math.max(windowEnd, app.endTime());
        }
        int cores = QueueAggregator.fixedCores(app);
        ContentionTimeline contention = ContentionTimeline.from(
                app.taskIntervals(), samples, cores, normalizedBucketMs, windowStart, windowEnd);
        return new QueueAggregator().aggregate(
                app, samples, contention, sourcePath, normalizedTopN, normalizedBucketMs, context);
    }
}
