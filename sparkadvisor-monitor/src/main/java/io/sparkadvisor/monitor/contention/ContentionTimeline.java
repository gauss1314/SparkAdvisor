package io.sparkadvisor.monitor.contention;

import io.sparkadvisor.core.model.TaskInterval;
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

    private ContentionTimeline(int totalCores,
                               List<Segment> segments,
                               List<BucketUtilization> bucketUtilization,
                               Map<Long, QueryContention> queryContention,
                               List<Window> hotspots) {
        this.totalCores = Math.max(1, totalCores);
        this.segments = List.copyOf(segments);
        this.bucketUtilization = List.copyOf(bucketUtilization);
        this.queryContention = Map.copyOf(queryContention);
        this.hotspots = List.copyOf(hotspots);
    }

    public static ContentionTimeline from(List<TaskInterval> tasks,
                                          List<QuerySample> queries,
                                          int totalCores,
                                          long bucketMs,
                                          long windowStart,
                                          long windowEnd) {
        int cores = Math.max(1, totalCores);
        long bucket = Math.max(60_000L, bucketMs);
        List<TaskInterval> validTasks = tasks == null ? List.of() : tasks.stream()
                .filter(t -> t.finishTime() > t.launchTime())
                .toList();
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

        List<BucketUtilization> buckets = buildBuckets(segments, bucket, windowStart, windowEnd, cores);
        Map<Long, QueryContention> contention = buildQueryContention(queries, segments, ownCoreMs, cores);
        List<Window> hot = buckets.stream()
                .filter(b -> b.avgUtilization() >= HOT_UTILIZATION)
                .map(b -> new Window(b.bucketStart(), b.bucketEnd(), b.avgUtilization()))
                .toList();

        // totalBusyCoreMs is intentionally computed during scan to keep the exact integral in
        // one place; segments carry the public time series used by downstream renderers.
        if (totalBusyCoreMs < 0) {
            throw new IllegalStateException("negative busy core-ms");
        }
        return new ContentionTimeline(cores, segments, buckets, contention, hot);
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
        return new ContentionTimeline(cores, List.of(), buckets, contention, List.of());
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
            result.put(q.executionId(),
                    new QueryContention(q.executionId(), avgPoolUtil, ownShare, own, limited));
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

    private static List<BucketUtilization> buildBuckets(List<Segment> segments, long bucketMs,
                                                        long windowStart, long windowEnd, int cores) {
        long start = floor(windowStart, bucketMs);
        long end = Math.max(windowEnd, start + bucketMs);
        List<BucketUtilization> result = new ArrayList<>();
        for (long bucketStart = start; bucketStart < end; bucketStart += bucketMs) {
            long bucketEnd = Math.min(bucketStart + bucketMs, end);
            long busy = integrateBusyCoreMs(segments, bucketStart, bucketEnd, cores);
            long capacity = (bucketEnd - bucketStart) * (long) cores;
            double avg = capacity <= 0 ? 0.0 : (double) busy / (double) capacity;
            result.add(new BucketUtilization(bucketStart, bucketEnd, avg));
        }
        return result;
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

    private record Event(long time, int delta, Long sqlExecutionId) {}

    public record Segment(long startTime, long endTime, int busyCores, double utilization) {}

    public record BucketUtilization(long bucketStart, long bucketEnd, double avgUtilization) {}

    public record QueryContention(
            long executionId,
            double avgPoolUtilization,
            double ownCoreShare,
            long ownCoreMs,
            boolean contentionLimited) {
    }

    public record Window(long startTime, long endTime, double avgUtilization) {}
}
