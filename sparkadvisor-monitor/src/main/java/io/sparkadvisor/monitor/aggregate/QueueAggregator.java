package io.sparkadvisor.monitor.aggregate;

import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.ExecutorEvent;
import io.sparkadvisor.core.model.TaskInterval;
import io.sparkadvisor.monitor.collect.QuerySample;
import io.sparkadvisor.monitor.contention.ContentionTimeline;
import io.sparkadvisor.monitor.rule.QueueRuleEngine;
import io.sparkadvisor.report.model.AnalysisResultBuilder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregates per-SQL samples plus the contention timeline into the queue-level contract.
 */
public final class QueueAggregator {

    private final QueueRuleEngine ruleEngine = new QueueRuleEngine();

    public QueueAnalysisResult aggregate(ApplicationModel app,
                                         List<QuerySample> samples,
                                         ContentionTimeline contention,
                                         String sourcePath,
                                         int topN,
                                         long bucketMs) {
        List<QuerySample> completed = samples.stream()
                .filter(q -> !q.running() && q.durationMs() > 0L)
                .toList();
        int running = (int) samples.stream().filter(QuerySample::running).count();
        int failed = (int) samples.stream().filter(QuerySample::failed).count();
        long windowStart = windowStart(app, samples);
        long windowEnd = windowEnd(app, samples);
        int cores = contention.totalCores();

        QueueAnalysisResult base = new QueueAnalysisResult(
                summary(app, samples.size(), completed.size(), running, failed, windowStart, windowEnd, cores),
                timeline(completed, contention, windowStart, windowEnd, bucketMs),
                bottlenecks(samples),
                utilization(contention),
                resources(completed),
                contentionReport(completed, contention),
                topSlowQueries(completed, contention, topN),
                List.of(),
                null,
                meta(app, sourcePath, topN));
        return base.withRecommendations(ruleEngine.recommend(base));
    }

    public static int fixedCores(ApplicationModel app) {
        int configured = readConfiguredCores(app.conf());
        int fromEvents = maxCoresFromEvents(app.executorEvents());
        return Math.max(1, Math.max(configured, fromEvents));
    }

    private QueueAnalysisResult.QueueSummary summary(ApplicationModel app, int total, int completed,
                                                     int running, int failed, long start, long end, int cores) {
        return new QueueAnalysisResult.QueueSummary(
                app.appId(),
                app.appName(),
                start,
                end,
                total,
                completed,
                running,
                failed,
                cores);
    }

    private List<QueueAnalysisResult.HourBucketStat> timeline(List<QuerySample> completed,
                                                              ContentionTimeline contention,
                                                              long windowStart,
                                                              long windowEnd,
                                                              long bucketMs) {
        long bucket = Math.max(60_000L, bucketMs);
        Map<Long, List<Long>> durationsByBucket = new LinkedHashMap<>();
        long start = floor(windowStart, bucket);
        long end = Math.max(windowEnd, windowStart + bucket);
        for (long t = start; t < end; t += bucket) {
            durationsByBucket.put(t, new ArrayList<>());
        }
        for (QuerySample q : completed) {
            long bucketStart = floor(q.startTime(), bucket);
            durationsByBucket.computeIfAbsent(bucketStart, k -> new ArrayList<>()).add(q.durationMs());
        }

        Map<Long, Double> utilByBucket = new HashMap<>();
        for (ContentionTimeline.BucketUtilization u : contention.bucketUtilization()) {
            utilByBucket.put(u.bucketStart(), u.avgUtilization());
        }

        List<QueueAnalysisResult.HourBucketStat> result = new ArrayList<>();
        for (Map.Entry<Long, List<Long>> entry : durationsByBucket.entrySet()) {
            long bucketStart = entry.getKey();
            List<Long> values = entry.getValue();
            result.add(new QueueAnalysisResult.HourBucketStat(
                    bucketStart,
                    bucketStart + bucket,
                    values.size(),
                    quantile(values, 0.50),
                    quantile(values, 0.95),
                    quantile(values, 0.99),
                    utilByBucket.getOrDefault(bucketStart, 0.0)));
        }
        return result;
    }

    private List<QueueAnalysisResult.BottleneckCluster> bottlenecks(List<QuerySample> samples) {
        List<QuerySample> analyzed = samples.stream().filter(QuerySample::deepAnalyzed).toList();
        int denominator = Math.max(1, analyzed.size());
        Map<String, RuleCount> counts = new HashMap<>();
        for (QuerySample sample : analyzed) {
            for (Finding finding : sample.findings()) {
                counts.computeIfAbsent(finding.ruleId(),
                                ignored -> new RuleCount(finding.ruleId(), finding.category()))
                        .affected++;
            }
        }
        return counts.values().stream()
                .sorted(Comparator.comparingInt((RuleCount c) -> c.affected).reversed()
                        .thenComparing(c -> c.ruleId))
                .map(c -> new QueueAnalysisResult.BottleneckCluster(
                        c.ruleId, c.category, c.affected, (double) c.affected / denominator))
                .toList();
    }

    private QueueAnalysisResult.UtilizationSeries utilization(ContentionTimeline contention) {
        List<QueueAnalysisResult.UtilizationSeries.Point> points = contention.bucketUtilization().stream()
                .map(u -> new QueueAnalysisResult.UtilizationSeries.Point(
                        u.bucketStart(), u.bucketEnd(), u.avgUtilization()))
                .toList();
        double avg = points.stream().mapToDouble(QueueAnalysisResult.UtilizationSeries.Point::avgUtilization)
                .average()
                .orElse(0.0);
        double peak = points.stream().mapToDouble(QueueAnalysisResult.UtilizationSeries.Point::avgUtilization)
                .max()
                .orElse(0.0);
        return new QueueAnalysisResult.UtilizationSeries(points, avg, peak);
    }

    private QueueAnalysisResult.ResourceMetrics resources(List<QuerySample> completed) {
        long totalSpill = completed.stream().mapToLong(QuerySample::spillBytes).sum();
        List<Long> gcBasisPoints = completed.stream()
                .map(q -> Math.round(q.maxGcRatio() * 10_000.0))
                .sorted()
                .toList();
        double avgGc = gcBasisPoints.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0) / 10_000.0;
        double p95Gc = quantile(gcBasisPoints, 0.95) / 10_000.0;
        double maxGc = gcBasisPoints.isEmpty() ? 0.0
                : gcBasisPoints.get(gcBasisPoints.size() - 1) / 10_000.0;
        return new QueueAnalysisResult.ResourceMetrics(totalSpill, avgGc, p95Gc, maxGc);
    }

    private QueueAnalysisResult.ContentionReport contentionReport(List<QuerySample> completed,
                                                                  ContentionTimeline contention) {
        long limited = completed.stream()
                .filter(q -> contention.queryContention()
                        .getOrDefault(q.executionId(), emptyContention(q.executionId()))
                        .contentionLimited())
                .count();
        double pct = completed.isEmpty() ? 0.0 : (double) limited / (double) completed.size();
        List<QueueAnalysisResult.ContentionReport.Window> hotspots = contention.hotspots().stream()
                .map(w -> new QueueAnalysisResult.ContentionReport.Window(
                        w.startTime(), w.endTime(), w.avgUtilization()))
                .toList();
        List<QueueAnalysisResult.SlowQueryRef> hogs = completed.stream()
                .sorted(Comparator.comparingLong((QuerySample q) -> contention.queryContention()
                        .getOrDefault(q.executionId(), emptyContention(q.executionId()))
                        .ownCoreMs()).reversed())
                .limit(10)
                .map(q -> slowRef(q, contention))
                .toList();
        return new QueueAnalysisResult.ContentionReport(pct, hotspots, hogs);
    }

    private List<QueueAnalysisResult.SlowQueryRef> topSlowQueries(List<QuerySample> completed,
                                                                  ContentionTimeline contention,
                                                                  int topN) {
        return completed.stream()
                .sorted(Comparator.comparingLong(QuerySample::durationMs).reversed())
                .limit(Math.max(1, topN))
                .map(q -> slowRef(q, contention))
                .toList();
    }

    private QueueAnalysisResult.SlowQueryRef slowRef(QuerySample q, ContentionTimeline contention) {
        ContentionTimeline.QueryContention c = contention.queryContention()
                .getOrDefault(q.executionId(), emptyContention(q.executionId()));
        return new QueueAnalysisResult.SlowQueryRef(
                q.statementId(),
                q.executionId(),
                q.startTime(),
                q.endTime(),
                q.durationMs(),
                dominantBottleneck(q),
                c.contentionLimited(),
                c.ownCoreMs());
    }

    private String dominantBottleneck(QuerySample q) {
        if (q.findings() == null || q.findings().isEmpty()) {
            return "none";
        }
        return q.findings().get(0).ruleId();
    }

    private QueueAnalysisResult.Meta meta(ApplicationModel app, String sourcePath, int topN) {
        return new QueueAnalysisResult.Meta(
                AnalysisResultBuilder.VERSION,
                Instant.now().toString(),
                app.incomplete(),
                app.incomplete(),
                sourcePath,
                topN,
                "Contention is inferred from task occupancy under a FIFO/single-pool assumption; "
                        + "event logs do not directly record queue wait time.");
    }

    private static long windowStart(ApplicationModel app, List<QuerySample> samples) {
        long minSql = samples.stream()
                .mapToLong(QuerySample::startTime)
                .filter(v -> v > 0L)
                .min()
                .orElse(0L);
        if (app.startTime() > 0L) {
            return minSql > 0L ? Math.min(app.startTime(), minSql) : app.startTime();
        }
        return minSql;
    }

    private static long windowEnd(ApplicationModel app, List<QuerySample> samples) {
        long maxSql = samples.stream()
                .mapToLong(q -> Math.max(q.endTime(), q.startTime()))
                .max()
                .orElse(0L);
        long maxTask = app.taskIntervals().stream()
                .mapToLong(TaskInterval::finishTime)
                .max()
                .orElse(0L);
        long appEnd = app.endTime() > 0L ? app.endTime() : 0L;
        return Math.max(appEnd, Math.max(maxSql, maxTask));
    }

    private static long floor(long value, long bucketMs) {
        if (value <= 0L) return 0L;
        return (value / bucketMs) * bucketMs;
    }

    private static long quantile(List<Long> values, double q) {
        if (values == null || values.isEmpty()) {
            return 0L;
        }
        List<Long> sorted = values.stream().sorted().toList();
        int idx = (int) Math.ceil(q * sorted.size()) - 1;
        idx = Math.max(0, Math.min(sorted.size() - 1, idx));
        return sorted.get(idx);
    }

    private static int readConfiguredCores(Map<String, String> conf) {
        int instances = parseInt(conf.get("spark.executor.instances"), 0);
        int cores = parseInt(conf.get("spark.executor.cores"), 1);
        int total = instances * cores;
        return total > 0 ? total : 1;
    }

    private static int maxCoresFromEvents(List<ExecutorEvent> events) {
        int current = 0;
        int max = 0;
        List<ExecutorEvent> sorted = events == null ? List.of() : events.stream()
                .sorted(Comparator.comparingLong(ExecutorEvent::timeMs))
                .toList();
        for (ExecutorEvent event : sorted) {
            current += event.added() ? event.cores() : -event.cores();
            if (current > max) {
                max = current;
            }
        }
        return Math.max(0, max);
    }

    private static int parseInt(String v, int dflt) {
        if (v == null) return dflt;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return dflt;
        }
    }

    private static ContentionTimeline.QueryContention emptyContention(long executionId) {
        return new ContentionTimeline.QueryContention(executionId, 0.0, 0.0, 0L, false);
    }

    private static final class RuleCount {
        final String ruleId;
        final String category;
        int affected;

        RuleCount(String ruleId, String category) {
            this.ruleId = ruleId;
            this.category = category;
        }
    }
}
