package io.sparkadvisor.monitor.aggregate;

import io.sparkadvisor.analyzer.RuleThresholds;
import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.ExecutorEvent;
import io.sparkadvisor.core.model.TaskInterval;
import io.sparkadvisor.core.util.Java8Collections;
import io.sparkadvisor.core.util.Strings;
import io.sparkadvisor.monitor.QueueAnalysisContext;
import io.sparkadvisor.monitor.collect.QuerySample;
import io.sparkadvisor.monitor.contention.ContentionTimeline;
import io.sparkadvisor.monitor.rule.QueueRuleEngine;
import io.sparkadvisor.report.model.AnalysisResultBuilder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Aggregates per-SQL samples plus the contention timeline into the queue-level contract.
 */
public final class QueueAggregator {

    private static final RuleThresholds LIGHT_RULE_THRESHOLDS = RuleThresholds.defaults();

    private final QueueRuleEngine ruleEngine = new QueueRuleEngine();

    public QueueAnalysisResult aggregate(ApplicationModel app,
                                         List<QuerySample> samples,
                                         ContentionTimeline contention,
                                         String sourcePath,
                                         int topN,
                                         long bucketMs) {
        return aggregate(app, samples, contention, sourcePath, topN, bucketMs,
                QueueAnalysisContext.defaults());
    }

    public QueueAnalysisResult aggregate(ApplicationModel app,
                                         List<QuerySample> samples,
                                         ContentionTimeline contention,
                                         String sourcePath,
                                         int topN,
                                         long bucketMs,
                                         QueueAnalysisContext context) {
        List<QuerySample> completed = samples.stream()
                .filter(q -> !q.running() && q.durationMs() > 0L)
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
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
                resources(completed, contention),
                contentionReport(completed, contention),
                topSlowQueries(completed, contention, topN),
                sampledQueries(completed, contention),
                templateStats(completed, contention),
                Java8Collections.<QueueAnalysisResult.QueueRecommendation>listOf(),
                null,
                meta(app, sourcePath, topN, samples, context));
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
        Map<Long, ContentionTimeline.BucketUtilization> resourceByBucket = new HashMap<>();
        for (ContentionTimeline.BucketUtilization u : contention.bucketUtilization()) {
            utilByBucket.put(u.bucketStart(), u.avgUtilization());
            resourceByBucket.put(u.bucketStart(), u);
        }

        List<QueueAnalysisResult.HourBucketStat> result = new ArrayList<>();
        for (Map.Entry<Long, List<Long>> entry : durationsByBucket.entrySet()) {
            long bucketStart = entry.getKey();
            List<Long> values = entry.getValue();
            ContentionTimeline.BucketUtilization resource = resourceByBucket.get(bucketStart);
            result.add(new QueueAnalysisResult.HourBucketStat(
                    bucketStart,
                    bucketStart + bucket,
                    values.size(),
                    quantile(values, 0.50),
                    quantile(values, 0.95),
                    quantile(values, 0.99),
                    utilByBucket.getOrDefault(bucketStart, 0.0),
                    resource == null ? 0.0 : resource.cpuEfficiency(),
                    resource == null ? 0.0 : resource.fetchWaitRatio(),
                    resource == null ? 0.0 : resource.gcRatio(),
                    resource == null ? 0.0 : resource.failedAttemptRatio(),
                    resource == null ? 0.0 : resource.speculativeAttemptRatio(),
                    resource == null ? 0 : resource.taskCount(),
                    resource == null ? 0 : resource.peakConcurrentTasks()));
        }
        return Java8Collections.listCopy(result);
    }

    private List<QueueAnalysisResult.BottleneckCluster> bottlenecks(List<QuerySample> samples) {
        List<QuerySample> completed = samples.stream()
                .filter(q -> !q.running() && q.durationMs() > 0L)
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
        List<QuerySample> analyzed = completed.stream()
                .filter(QuerySample::deepAnalyzed)
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
        int lightDenominator = Math.max(1, completed.size());
        int deepDenominator = Math.max(1, analyzed.size());
        double coverage = samples.isEmpty() ? 0.0 : (double) analyzed.size() / (double) samples.size();
        Map<String, RuleCount> counts = new HashMap<>();
        for (QuerySample sample : completed) {
            for (RuleSignal signal : lightSignals(sample)) {
                addRule(counts, signal.ruleId, signal.category, sample.executionId(), true);
            }
        }
        for (QuerySample sample : analyzed) {
            for (Finding finding : sample.findings()) {
                addRule(counts, finding.ruleId(), finding.category(), sample.executionId(), false);
            }
        }
        return Java8Collections.listCopy(counts.values().stream()
                .sorted(Comparator.comparingInt((RuleCount c) -> c.affected()).reversed()
                        .thenComparing(c -> c.ruleId))
                .map(c -> new QueueAnalysisResult.BottleneckCluster(
                        c.ruleId, c.category, c.affected(), c.affectedPct(lightDenominator, deepDenominator),
                        c.scope(), c.sampleCoveragePct(coverage)))
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new)));
    }

    private static void addRule(Map<String, RuleCount> counts, String ruleId, String category,
                                long executionId, boolean lightEvidence) {
        RuleCount count = counts.computeIfAbsent(ruleId, ignored -> new RuleCount(ruleId, category));
        count.add(executionId, lightEvidence);
    }

    private static List<RuleSignal> lightSignals(QuerySample sample) {
        List<RuleSignal> signals = new ArrayList<RuleSignal>();
        SqlAnalysis analysis = sample.sqlAnalysis();
        if (analysis == null) {
            return signals;
        }

        RuleThresholds t = LIGHT_RULE_THRESHOLDS;
        if (sample.coreUtilization() > 0.0 && sample.coreUtilization() < t.coreUtilLow()) {
            signals.add(new RuleSignal("R3_LOW_PARALLELISM", "parallelism"));
        }

        String plan = analysis.physicalPlanText();
        if (!Strings.isBlank(plan)) {
            boolean hasSortMergeJoin = plan.contains("SortMergeJoin");
            boolean hasBroadcastJoin = plan.contains("BroadcastHashJoin")
                    || plan.contains("BroadcastNestedLoopJoin");
            if (hasSortMergeJoin && !hasBroadcastJoin) {
                signals.add(new RuleSignal("R7_BROADCAST_JOIN", "join"));
            }
            boolean hasSort = plan.contains("Sort");
            boolean hasAggregate = plan.contains("HashAggregate")
                    || plan.contains("ObjectHashAggregate")
                    || plan.contains("SortAggregate");
            if ((hasSort || hasAggregate) && sample.spillBytes() > 0L) {
                signals.add(new RuleSignal("R11_SORT_AGG_SPILL", "plan"));
            }
        }

        Set<String> stageLevelRules = new HashSet<String>();
        for (StageAnalysis st : analysis.stages()) {
            if (st.skewRatio() >= t.skewRatioWarn()
                    || st.shuffleSkewRatio() >= t.shuffleSkewWarn()) {
                stageLevelRules.add("R1_DATA_SKEW|skew");
            }
            if (st.spillBytes() > 0L
                    && (double) st.spillBytes() / (double) Math.max(st.shuffleReadBytes(), 1L)
                    >= t.spillRatioWarn()) {
                stageLevelRules.add("R2_EXCESSIVE_SPILL|spill");
            }
            if (st.medianTaskMs() > 0L && st.medianTaskMs() < t.smallTaskMedianMs()
                    && st.numTasks() >= t.overParallelMinTasks()) {
                stageLevelRules.add("R4_OVER_PARALLELISM|parallelism");
            }
            if (st.inputBytes() > 0L && st.numTasks() >= t.overParallelMinTasks()
                    && st.medianInputBytesPerTask() > 0L
                    && st.medianInputBytesPerTask() < t.smallInputPerTaskBytes()) {
                stageLevelRules.add("R5_SMALL_FILES|small-files");
            }
            if (st.gcRatio() >= t.gcRatioWarn()) {
                stageLevelRules.add("R6_GC_PRESSURE|gc");
            }
            if (st.wallClockMs() > 0L && st.schedulingDelayMs() > 0L
                    && (double) st.schedulingDelayMs() / (double) st.wallClockMs()
                    >= t.schedulingDelayRatioWarn()) {
                stageLevelRules.add("R8_SCHEDULING_DELAY|scheduling");
            }
            if (st.shuffleReadBytes() > 0L && st.shuffleFetchWaitMs() > 0L
                    && st.totalTaskTimeMs() > 0L
                    && (double) st.shuffleFetchWaitMs() / (double) st.totalTaskTimeMs()
                    >= t.shuffleFetchWaitRatioWarn()) {
                stageLevelRules.add("R9_SHUFFLE_FETCH_WAIT|shuffle");
            }
            double extraRatio = st.numTasks() <= 0 ? 0.0
                    : (double) st.extraTaskAttempts() / (double) st.numTasks();
            if (st.failedTaskAttempts() >= t.failedTaskAttemptsWarn()
                    || extraRatio >= t.extraTaskAttemptRatioWarn()) {
                stageLevelRules.add("R10_TASK_RETRY|reliability");
            }
        }
        for (String signal : stageLevelRules) {
            int split = signal.indexOf('|');
            signals.add(new RuleSignal(signal.substring(0, split), signal.substring(split + 1)));
        }
        return signals;
    }

    private QueueAnalysisResult.UtilizationSeries utilization(ContentionTimeline contention) {
        List<QueueAnalysisResult.UtilizationSeries.Point> points = contention.bucketUtilization().stream()
                .map(u -> new QueueAnalysisResult.UtilizationSeries.Point(
                        u.bucketStart(), u.bucketEnd(), u.avgUtilization()))
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
        double avg = points.stream().mapToDouble(QueueAnalysisResult.UtilizationSeries.Point::avgUtilization)
                .average()
                .orElse(0.0);
        double peak = points.stream().mapToDouble(QueueAnalysisResult.UtilizationSeries.Point::avgUtilization)
                .max()
                .orElse(0.0);
        return new QueueAnalysisResult.UtilizationSeries(Java8Collections.listCopy(points), avg, peak);
    }

    private QueueAnalysisResult.ResourceMetrics resources(List<QuerySample> completed,
                                                          ContentionTimeline contention) {
        long totalSpill = completed.stream().mapToLong(QuerySample::spillBytes).sum();
        List<Long> gcBasisPoints = completed.stream()
                .map(q -> Math.round(q.maxGcRatio() * 10_000.0))
                .sorted()
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
        double avgGc = gcBasisPoints.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0) / 10_000.0;
        double p95Gc = quantile(gcBasisPoints, 0.95) / 10_000.0;
        double maxGc = gcBasisPoints.isEmpty() ? 0.0
                : gcBasisPoints.get(gcBasisPoints.size() - 1) / 10_000.0;
        double avgSlot = contention.bucketUtilization().stream()
                .mapToDouble(ContentionTimeline.BucketUtilization::avgUtilization)
                .average().orElse(0.0);
        double avgCpu = contention.bucketUtilization().stream()
                .mapToDouble(ContentionTimeline.BucketUtilization::cpuEfficiency)
                .average().orElse(0.0);
        double avgFetch = contention.bucketUtilization().stream()
                .mapToDouble(ContentionTimeline.BucketUtilization::fetchWaitRatio)
                .average().orElse(0.0);
        double avgBucketGc = contention.bucketUtilization().stream()
                .mapToDouble(ContentionTimeline.BucketUtilization::gcRatio)
                .average().orElse(0.0);
        double failedAttemptRatio = completed.stream().mapToInt(QuerySample::failedTaskAttempts).sum()
                / Math.max(1.0, completed.stream().mapToInt(q -> q.failedTaskAttempts() + q.extraTaskAttempts() + Math.max(1, q.stageCount())).sum());
        double speculativeAttemptRatio = completed.stream().mapToInt(QuerySample::extraTaskAttempts).sum()
                / Math.max(1.0, completed.stream().mapToInt(q -> q.failedTaskAttempts() + q.extraTaskAttempts() + Math.max(1, q.stageCount())).sum());
        return new QueueAnalysisResult.ResourceMetrics(totalSpill, avgGc, p95Gc, maxGc,
                avgSlot, avgCpu, avgFetch, avgBucketGc, failedAttemptRatio, speculativeAttemptRatio);
    }

    private QueueAnalysisResult.ContentionReport contentionReport(List<QuerySample> completed,
                                                                  ContentionTimeline contention) {
        long limited = completed.stream()
                .filter(q -> contention.queryContention()
                        .getOrDefault(q.executionId(), emptyContention(q.executionId()))
                        .contentionLimited())
                .count();
        long inefficient = completed.stream()
                .filter(q -> contention.queryContention()
                        .getOrDefault(q.executionId(), emptyContention(q.executionId()))
                        .inefficientBusy())
                .count();
        double pct = completed.isEmpty() ? 0.0 : (double) limited / (double) completed.size();
        double inefficientPct = completed.isEmpty() ? 0.0 : (double) inefficient / (double) completed.size();
        List<QueueAnalysisResult.ContentionReport.Window> hotspots = contention.hotspots().stream()
                .map(w -> new QueueAnalysisResult.ContentionReport.Window(
                        w.startTime(), w.endTime(), w.avgUtilization()))
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
        List<QueueAnalysisResult.SlowQueryRef> hogs = completed.stream()
                .sorted(Comparator.comparingLong((QuerySample q) -> contention.queryContention()
                        .getOrDefault(q.executionId(), emptyContention(q.executionId()))
                        .ownCoreMs()).reversed())
                .limit(10)
                .map(q -> slowRef(q, contention))
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
        List<QueueAnalysisResult.ContentionReport.Window> starvation = contention.starvationWindows().stream()
                .map(w -> new QueueAnalysisResult.ContentionReport.Window(
                        w.startTime(), w.endTime(), w.avgUtilization()))
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
        return new QueueAnalysisResult.ContentionReport(pct, inefficientPct, Java8Collections.listCopy(hotspots),
                Java8Collections.listCopy(starvation),
                Java8Collections.listCopy(hogs));
    }

    private List<QueueAnalysisResult.SlowQueryRef> topSlowQueries(List<QuerySample> completed,
                                                                  ContentionTimeline contention,
                                                                  int topN) {
        return Java8Collections.listCopy(completed.stream()
                .sorted(Comparator.comparingLong(QuerySample::durationMs).reversed())
                .limit(Math.max(1, topN))
                .map(q -> slowRef(q, contention))
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new)));
    }

    private List<QueueAnalysisResult.SlowQueryRef> sampledQueries(List<QuerySample> completed,
                                                                  ContentionTimeline contention) {
        return Java8Collections.listCopy(completed.stream()
                .filter(QuerySample::deepAnalyzed)
                .sorted(Comparator.comparingLong(QuerySample::durationMs).reversed())
                .map(q -> slowRef(q, contention))
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new)));
    }

    private List<QueueAnalysisResult.TemplateStat> templateStats(List<QuerySample> completed,
                                                                 ContentionTimeline contention) {
        Map<String, TemplateAccumulator> byTemplate = new LinkedHashMap<String, TemplateAccumulator>();
        for (QuerySample q : completed) {
            String rawTemplateHash = q.templateHash();
            final String templateHash = Strings.isBlank(rawTemplateHash)
                    ? "execution-" + q.executionId()
                    : rawTemplateHash;
            TemplateAccumulator acc = byTemplate.computeIfAbsent(templateHash,
                    ignored -> new TemplateAccumulator(templateHash, q.statementId()));
            ContentionTimeline.QueryContention c = contention.queryContention()
                    .getOrDefault(q.executionId(), emptyContention(q.executionId()));
            long coreMs = c.ownCoreMs() > 0L ? c.ownCoreMs() : q.totalTaskTimeMs();
            acc.add(q.durationMs(), coreMs, q.inputBytes(), q.shuffleReadBytes());
        }
        return Java8Collections.listCopy(byTemplate.values().stream()
                .sorted(Comparator.comparingLong(TemplateAccumulator::totalCoreMs).reversed()
                        .thenComparing(Comparator.comparingLong(TemplateAccumulator::totalDurationMs).reversed()))
                .limit(20)
                .map(TemplateAccumulator::toStat)
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new)));
    }

    private QueueAnalysisResult.SlowQueryRef slowRef(QuerySample q, ContentionTimeline contention) {
        ContentionTimeline.QueryContention c = contention.queryContention()
                .getOrDefault(q.executionId(), emptyContention(q.executionId()));
        return new QueueAnalysisResult.SlowQueryRef(
                q.statementId(),
                q.templateHash(),
                q.executionId(),
                q.startTime(),
                q.endTime(),
                q.durationMs(),
                dominantBottleneck(q),
                c.contentionLimited(),
                c.ownCoreMs(),
                q.deepAnalyzed());
    }

    private String dominantBottleneck(QuerySample q) {
        if (q.findings() == null || q.findings().isEmpty()) {
            return "none";
        }
        return q.findings().get(0).ruleId();
    }

    private QueueAnalysisResult.Meta meta(ApplicationModel app, String sourcePath, int topN,
                                          List<QuerySample> samples, QueueAnalysisContext context) {
        int deep = (int) samples.stream().filter(QuerySample::deepAnalyzed).count();
        double coverage = samples.isEmpty() ? 0.0 : (double) deep / (double) samples.size();
        QueueAnalysisContext safeContext = context == null ? QueueAnalysisContext.defaults() : context;
        String snapshotKey = safeContext.hasSnapshotKey()
                ? safeContext.snapshotKey()
                : snapshotKey(app, sourcePath);
        String degradedReason = safeContext.degradedReason();
        if (Strings.isBlank(degradedReason) && !safeContext.incremental() && app.incomplete()) {
            degradedReason = "Full snapshot replay fallback was used for an incomplete event log.";
        }
        return new QueueAnalysisResult.Meta(
                AnalysisResultBuilder.VERSION,
                Instant.now().toString(),
                app.incomplete(),
                app.incomplete(),
                safeContext.incremental(),
                sourcePath,
                snapshotKey,
                topN,
                samples.size(),
                deep,
                coverage,
                "topN+spill/fetch/GC/skew/template strata",
                "DEFAULT_REDACTION",
                degradedReason,
                "Contention is inferred from task occupancy under a FIFO/single-pool assumption; "
                        + "event logs do not directly record queue wait time.");
    }

    private static String snapshotKey(ApplicationModel app, String sourcePath) {
        String source = sourcePath == null ? "" : sourcePath;
        return app.appId() + ":" + Integer.toHexString(source.hashCode()) + ":" + app.endTime() + ":" + app.incomplete();
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
        List<Long> sorted = values.stream().sorted().collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
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
        List<ExecutorEvent> sorted = events == null ? Java8Collections.<ExecutorEvent>listOf() : events.stream()
                .sorted(Comparator.comparingLong(ExecutorEvent::timeMs))
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
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
        final Set<Long> affectedExecutions = new HashSet<Long>();
        boolean hasLightEvidence;
        boolean hasDeepEvidence;

        RuleCount(String ruleId, String category) {
            this.ruleId = ruleId;
            this.category = category;
        }

        void add(long executionId, boolean lightEvidence) {
            affectedExecutions.add(executionId);
            if (lightEvidence) {
                hasLightEvidence = true;
            } else {
                hasDeepEvidence = true;
            }
        }

        int affected() {
            return affectedExecutions.size();
        }

        double affectedPct(int lightDenominator, int deepDenominator) {
            int denominator = hasLightEvidence ? lightDenominator : deepDenominator;
            return (double) affected() / (double) Math.max(1, denominator);
        }

        double sampleCoveragePct(double deepCoverage) {
            return hasLightEvidence ? 1.0 : deepCoverage;
        }

        String scope() {
            if (hasLightEvidence && hasDeepEvidence) {
                return "FULL_QUEUE_LIGHT+DEEP_SAMPLE";
            }
            return hasLightEvidence ? "FULL_QUEUE_LIGHT" : "DEEP_SAMPLE";
        }
    }

    private static final class RuleSignal {
        final String ruleId;
        final String category;

        RuleSignal(String ruleId, String category) {
            this.ruleId = ruleId;
            this.category = category;
        }
    }

    private static final class TemplateAccumulator {
        final String templateHash;
        final String exampleStatementId;
        int queryCount;
        long totalDurationMs;
        long totalCoreMs;
        long totalInputBytes;
        long totalShuffleReadBytes;

        TemplateAccumulator(String templateHash, String exampleStatementId) {
            this.templateHash = templateHash;
            this.exampleStatementId = exampleStatementId;
        }

        void add(long durationMs, long coreMs, long inputBytes, long shuffleReadBytes) {
            queryCount++;
            totalDurationMs += Math.max(0L, durationMs);
            totalCoreMs += Math.max(0L, coreMs);
            totalInputBytes += Math.max(0L, inputBytes);
            totalShuffleReadBytes += Math.max(0L, shuffleReadBytes);
        }

        long totalCoreMs() {
            return totalCoreMs;
        }

        long totalDurationMs() {
            return totalDurationMs;
        }

        QueueAnalysisResult.TemplateStat toStat() {
            return new QueueAnalysisResult.TemplateStat(templateHash, exampleStatementId, queryCount,
                    totalDurationMs, totalCoreMs, totalInputBytes, totalShuffleReadBytes);
        }
    }
}
