package io.sparkadvisor.monitor.contention;

import io.sparkadvisor.core.model.TaskInterval;
import io.sparkadvisor.core.util.Java8Collections;
import io.sparkadvisor.core.util.ValueObjects;
import io.sparkadvisor.monitor.collect.QuerySample;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Queue-level scanline over all task intervals, attributing fixed-core pool occupancy to SQL
 * executions. The inference assumes a single FIFO-like pool; FAIR/multi-pool deployments should
 * treat contention classifications as lower-confidence estimates.
 */
public final class ContentionTimeline {

    private static final double HOT_UTILIZATION = 0.95;

    private final int totalCores;
    private final List<Segment> segments;
    private final List<BucketUtilization> bucketUtilization;
    private final Map<Long, QueryContention> queryContention;
    private final List<Window> hotspots;
    private final List<Window> starvationWindows;

    private ContentionTimeline(int totalCores,
                               List<Segment> segments,
                               List<BucketUtilization> bucketUtilization,
                               Map<Long, QueryContention> queryContention,
                               List<Window> hotspots,
                               List<Window> starvationWindows) {
        this.totalCores = Math.max(1, totalCores);
        this.segments = Java8Collections.listCopy(segments);
        this.bucketUtilization = Java8Collections.listCopy(bucketUtilization);
        this.queryContention = Java8Collections.mapCopy(queryContention);
        this.hotspots = Java8Collections.listCopy(hotspots);
        this.starvationWindows = Java8Collections.listCopy(starvationWindows);
    }

    public static ContentionTimeline from(List<TaskInterval> tasks,
                                          List<QuerySample> queries,
                                          int totalCores,
                                          long bucketMs,
                                          long windowStart,
                                          long windowEnd) {
        int cores = Math.max(1, totalCores);
        long bucket = Math.max(60_000L, bucketMs);
        List<TaskInterval> validTasks = tasks == null ? Java8Collections.<TaskInterval>listOf() : tasks.stream()
                .filter(t -> t.finishTime() > t.launchTime())
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
        if (validTasks.isEmpty()) {
            return empty(queries, cores, bucket, windowStart, windowEnd);
        }

        List<Event> events = new ArrayList<>(validTasks.size() * 2);
        for (TaskInterval task : validTasks) {
            events.add(new Event(task.launchTime(), +1, task.sqlExecutionId()));
            events.add(new Event(task.finishTime(), -1, task.sqlExecutionId()));
        }
        events.sort(Comparator
                .comparingLong(Event::time)
                .thenComparingInt(Event::delta));

        Map<Long, Integer> activeBySql = new HashMap<>();
        Map<Long, Long> ownCoreMs = new HashMap<>();
        List<Segment> segments = new ArrayList<>();
        long totalBusyCoreMs = 0L;
        long prev = events.get(0).time();
        int busy = 0;

        int i = 0;
        while (i < events.size()) {
            long time = events.get(i).time();
            if (time > prev && busy > 0) {
                long duration = time - prev;
                int cappedBusy = Math.min(busy, cores);
                segments.add(new Segment(prev, time, cappedBusy, utilization(cappedBusy, cores)));
                totalBusyCoreMs += (long) cappedBusy * duration;
                for (Map.Entry<Long, Integer> entry : activeBySql.entrySet()) {
                    int ownBusy = Math.min(entry.getValue(), cores);
                    ownCoreMs.merge(entry.getKey(), (long) ownBusy * duration, Long::sum);
                }
            }
            while (i < events.size() && events.get(i).time() == time) {
                Event event = events.get(i);
                Long sqlId = event.sqlExecutionId();
                if (event.delta() > 0) {
                    busy++;
                    if (sqlId != null) {
                        activeBySql.merge(sqlId, 1, Integer::sum);
                    }
                } else {
                    busy = Math.max(0, busy - 1);
                    if (sqlId != null) {
                        activeBySql.computeIfPresent(sqlId, (k, v) -> v <= 1 ? null : v - 1);
                    }
                }
                i++;
            }
            prev = time;
        }

        List<BucketUtilization> buckets = buildBuckets(segments, validTasks, bucket, windowStart, windowEnd, cores);
        Map<Long, QueryContention> contention = buildQueryContention(queries, segments, ownCoreMs, cores);
        List<Window> hot = buckets.stream()
                .filter(b -> b.avgUtilization() >= HOT_UTILIZATION)
                .map(b -> new Window(b.bucketStart(), b.bucketEnd(), b.avgUtilization()))
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
        List<Window> starvation = contention.values().stream()
                .filter(q -> q.contentionLimited() && q.ownCoreShare() <= 0.20)
                .map(q -> queryWindow(queries, q.executionId(), q.avgPoolUtilization()))
                .filter(w -> w != null)
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));

        // totalBusyCoreMs is intentionally computed during scan to keep the exact integral in
        // one place; segments carry the public time series used by downstream renderers.
        if (totalBusyCoreMs < 0) {
            throw new IllegalStateException("negative busy core-ms");
        }
        return new ContentionTimeline(cores, segments, buckets, contention, hot, starvation);
    }

    private static ContentionTimeline empty(List<QuerySample> queries, int cores, long bucketMs,
                                            long windowStart, long windowEnd) {
        List<BucketUtilization> buckets = new ArrayList<>();
        long start = floor(windowStart, bucketMs);
        for (long t = start; t < Math.max(windowEnd, start + bucketMs); t += bucketMs) {
            buckets.add(new BucketUtilization(t, Math.min(t + bucketMs, windowEnd), 0.0));
        }
        Map<Long, QueryContention> contention = new LinkedHashMap<>();
        if (queries != null) {
            for (QuerySample q : queries) {
                contention.put(q.executionId(),
                        new QueryContention(q.executionId(), 0.0, 0.0, 0L, false));
            }
        }
        return new ContentionTimeline(cores, Java8Collections.<Segment>listOf(), buckets, contention,
                Java8Collections.<Window>listOf(), Java8Collections.<Window>listOf());
    }

    private static Map<Long, QueryContention> buildQueryContention(List<QuerySample> queries,
                                                                   List<Segment> segments,
                                                                   Map<Long, Long> ownCoreMs,
                                                                   int cores) {
        Map<Long, QueryContention> result = new LinkedHashMap<>();
        if (queries == null) {
            return result;
        }
        for (QuerySample q : queries) {
            if (q.startTime() <= 0L || q.endTime() <= q.startTime()) {
                result.put(q.executionId(),
                        new QueryContention(q.executionId(), 0.0, 0.0, 0L, false));
                continue;
            }
            long capacity = (q.endTime() - q.startTime()) * (long) cores;
            long poolBusy = integrateBusyCoreMs(segments, q.startTime(), q.endTime(), cores);
            long own = ownCoreMs.getOrDefault(q.executionId(), 0L);
            double avgPoolUtil = capacity <= 0 ? 0.0 : (double) poolBusy / (double) capacity;
            double ownShare = capacity <= 0 ? 0.0 : (double) own / (double) capacity;
            boolean limited = avgPoolUtil >= 0.85 && ownShare <= 0.50 && own > 0L;
            boolean inefficientBusy = avgPoolUtil >= 0.85
                    && (q.fetchWaitRatio() >= 0.20 || q.maxGcRatio() >= 0.10
                    || q.failedTaskAttempts() > 0 || q.extraTaskAttempts() > 0);
            result.put(q.executionId(),
                    new QueryContention(q.executionId(), avgPoolUtil, ownShare, own, limited, inefficientBusy));
        }
        return result;
    }

    private static long integrateBusyCoreMs(List<Segment> segments, long start, long end, int cores) {
        long total = 0L;
        for (Segment s : segments) {
            if (s.endTime() <= start) continue;
            if (s.startTime() >= end) break;
            long overlapStart = Math.max(start, s.startTime());
            long overlapEnd = Math.min(end, s.endTime());
            if (overlapEnd > overlapStart) {
                total += Math.min(s.busyCores(), cores) * (overlapEnd - overlapStart);
            }
        }
        return total;
    }

    private static List<BucketUtilization> buildBuckets(List<Segment> segments, List<TaskInterval> tasks, long bucketMs,
                                                        long windowStart, long windowEnd, int cores) {
        long start = floor(windowStart, bucketMs);
        long end = Math.max(windowEnd, start + bucketMs);
        List<BucketUtilization> result = new ArrayList<>();
        for (long bucketStart = start; bucketStart < end; bucketStart += bucketMs) {
            long bucketEnd = Math.min(bucketStart + bucketMs, end);
            long busy = integrateBusyCoreMs(segments, bucketStart, bucketEnd, cores);
            long capacity = (bucketEnd - bucketStart) * (long) cores;
            double avg = capacity <= 0 ? 0.0 : (double) busy / (double) capacity;
            BucketMetrics m = bucketMetrics(tasks, bucketStart, bucketEnd);
            result.add(new BucketUtilization(bucketStart, bucketEnd, avg,
                    m.cpuEfficiency(), m.fetchWaitRatio(), m.gcRatio(),
                    m.failedAttemptRatio(), m.speculativeAttemptRatio()));
        }
        return result;
    }

    private static BucketMetrics bucketMetrics(List<TaskInterval> tasks, long bucketStart, long bucketEnd) {
        long runMs = 0L;
        long cpuMs = 0L;
        long fetchMs = 0L;
        long gcMs = 0L;
        int attempts = 0;
        int failed = 0;
        int speculative = 0;
        for (TaskInterval task : tasks) {
            if (task.finishTime() <= bucketStart || task.launchTime() >= bucketEnd) {
                continue;
            }
            long taskDuration = Math.max(1L, task.durationMs());
            long overlap = Math.max(0L, Math.min(task.finishTime(), bucketEnd) - Math.max(task.launchTime(), bucketStart));
            if (overlap <= 0L) {
                continue;
            }
            long taskRun = task.executorRunTimeMs() > 0L ? task.executorRunTimeMs() : taskDuration;
            double share = (double) overlap / (double) taskDuration;
            runMs += Math.round(taskRun * share);
            cpuMs += Math.round((task.executorCpuTimeNs() / 1_000_000.0) * share);
            fetchMs += Math.round(task.shuffleFetchWaitMs() * share);
            gcMs += Math.round(task.jvmGcTimeMs() * share);
            if (task.launchTime() >= bucketStart && task.launchTime() < bucketEnd) {
                attempts++;
                if (task.failedAttempt()) {
                    failed++;
                }
                if (task.speculativeAttempt()) {
                    speculative++;
                }
            }
        }
        return new BucketMetrics(runMs, cpuMs, fetchMs, gcMs, attempts, failed, speculative);
    }

    private static Window queryWindow(List<QuerySample> queries, long executionId, double avgUtilization) {
        if (queries == null) {
            return null;
        }
        for (QuerySample query : queries) {
            if (query.executionId() == executionId && query.startTime() > 0L && query.endTime() > query.startTime()) {
                return new Window(query.startTime(), query.endTime(), avgUtilization);
            }
        }
        return null;
    }

    private static long floor(long value, long bucketMs) {
        if (value <= 0L) return 0L;
        return (value / bucketMs) * bucketMs;
    }

    private static double utilization(int busy, int cores) {
        return cores <= 0 ? 0.0 : Math.min(1.0, (double) busy / (double) cores);
    }

    public int totalCores() {
        return totalCores;
    }

    public List<Segment> segments() {
        return segments;
    }

    public List<BucketUtilization> bucketUtilization() {
        return bucketUtilization;
    }

    public Map<Long, QueryContention> queryContention() {
        return queryContention;
    }

    public List<Window> hotspots() {
        return hotspots;
    }

    public List<Window> starvationWindows() {
        return starvationWindows;
    }

    private static final class Event { private final long time; private final int delta; private final Long sqlExecutionId; Event(long time,int delta,Long sqlExecutionId){this.time=time;this.delta=delta;this.sqlExecutionId=sqlExecutionId;} long time(){return time;} int delta(){return delta;} Long sqlExecutionId(){return sqlExecutionId;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);} }

    public static final class Segment { private final long startTime,endTime; private final int busyCores; private final double utilization; public Segment(long startTime,long endTime,int busyCores,double utilization){this.startTime=startTime;this.endTime=endTime;this.busyCores=busyCores;this.utilization=utilization;} public long startTime(){return startTime;} public long endTime(){return endTime;} public int busyCores(){return busyCores;} public double utilization(){return utilization;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);} }

    public static final class BucketUtilization { private final long bucketStart,bucketEnd; private final double avgUtilization,cpuEfficiency,fetchWaitRatio,gcRatio,failedAttemptRatio,speculativeAttemptRatio; public BucketUtilization(long bucketStart,long bucketEnd,double avgUtilization){this(bucketStart,bucketEnd,avgUtilization,0.0,0.0,0.0,0.0,0.0);} public BucketUtilization(long bucketStart,long bucketEnd,double avgUtilization,double cpuEfficiency,double fetchWaitRatio,double gcRatio,double failedAttemptRatio,double speculativeAttemptRatio){this.bucketStart=bucketStart;this.bucketEnd=bucketEnd;this.avgUtilization=avgUtilization;this.cpuEfficiency=cpuEfficiency;this.fetchWaitRatio=fetchWaitRatio;this.gcRatio=gcRatio;this.failedAttemptRatio=failedAttemptRatio;this.speculativeAttemptRatio=speculativeAttemptRatio;} public long bucketStart(){return bucketStart;} public long bucketEnd(){return bucketEnd;} public double avgUtilization(){return avgUtilization;} public double cpuEfficiency(){return cpuEfficiency;} public double fetchWaitRatio(){return fetchWaitRatio;} public double gcRatio(){return gcRatio;} public double failedAttemptRatio(){return failedAttemptRatio;} public double speculativeAttemptRatio(){return speculativeAttemptRatio;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);} }

    public static final class QueryContention { private final long executionId,ownCoreMs; private final double avgPoolUtilization,ownCoreShare; private final boolean contentionLimited,inefficientBusy; public QueryContention(long executionId,double avgPoolUtilization,double ownCoreShare,long ownCoreMs,boolean contentionLimited){this(executionId,avgPoolUtilization,ownCoreShare,ownCoreMs,contentionLimited,false);} public QueryContention(long executionId,double avgPoolUtilization,double ownCoreShare,long ownCoreMs,boolean contentionLimited,boolean inefficientBusy){this.executionId=executionId;this.avgPoolUtilization=avgPoolUtilization;this.ownCoreShare=ownCoreShare;this.ownCoreMs=ownCoreMs;this.contentionLimited=contentionLimited;this.inefficientBusy=inefficientBusy;} public long executionId(){return executionId;} public double avgPoolUtilization(){return avgPoolUtilization;} public double ownCoreShare(){return ownCoreShare;} public long ownCoreMs(){return ownCoreMs;} public boolean contentionLimited(){return contentionLimited;} public boolean inefficientBusy(){return inefficientBusy;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);} }

    public static final class Window { private final long startTime,endTime; private final double avgUtilization; public Window(long startTime,long endTime,double avgUtilization){this.startTime=startTime;this.endTime=endTime;this.avgUtilization=avgUtilization;} public long startTime(){return startTime;} public long endTime(){return endTime;} public double avgUtilization(){return avgUtilization;} @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);} @Override public int hashCode(){return ValueObjects.hashFields(this);} @Override public String toString(){return ValueObjects.toString(this);} }

    private static final class BucketMetrics { private final long runMs,cpuMs,fetchMs,gcMs; private final int attempts,failed,speculative; BucketMetrics(long runMs,long cpuMs,long fetchMs,long gcMs,int attempts,int failed,int speculative){this.runMs=runMs;this.cpuMs=cpuMs;this.fetchMs=fetchMs;this.gcMs=gcMs;this.attempts=attempts;this.failed=failed;this.speculative=speculative;} double cpuEfficiency(){return runMs<=0L?0.0:Math.min(1.0,(double)cpuMs/(double)runMs);} double fetchWaitRatio(){return runMs<=0L?0.0:(double)fetchMs/(double)runMs;} double gcRatio(){return runMs<=0L?0.0:(double)gcMs/(double)runMs;} double failedAttemptRatio(){return attempts<=0?0.0:(double)failed/(double)attempts;} double speculativeAttemptRatio(){return attempts<=0?0.0:(double)speculative/(double)attempts;} }
}
