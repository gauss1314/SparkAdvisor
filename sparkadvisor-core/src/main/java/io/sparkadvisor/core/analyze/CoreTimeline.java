package io.sparkadvisor.core.analyze;

import io.sparkadvisor.core.model.ExecutorEvent;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Reconstructs the number of available cores over time from {@link ExecutorEvent}s, and
 * integrates "core-milliseconds" of capacity over an arbitrary window.
 *
 * <p>This replaces the coarse {@code instances * cores} configuration estimate with the
 * actual capacity the application held during a SQL's execution (which varies with dynamic
 * allocation). Utilization = total task time / core-ms of capacity over the SQL's wall clock.
 *
 * <p>Pure data + arithmetic; no Spark types, fully unit-testable.
 */
public final class CoreTimeline {

    /** A segment [startMs, endMs) during which {@code cores} cores were available. */
    public record Segment(long startMs, long endMs, int cores) {
        long durationMs() {
            return Math.max(0, endMs - startMs);
        }
    }

    private final List<Segment> segments;
    private final int fallbackCores;

    private CoreTimeline(List<Segment> segments, int fallbackCores) {
        this.segments = segments;
        this.fallbackCores = Math.max(1, fallbackCores);
    }

    /**
     * Build from executor events. If no events were captured (e.g. logging disabled), the
     * timeline is empty and {@link #coreMillis} falls back to {@code fallbackCores * window}.
     *
     * @param events        executor add/remove events (any order)
     * @param fallbackCores cores to assume when no events are available
     */
    public static CoreTimeline from(List<ExecutorEvent> events, int fallbackCores) {
        if (events == null || events.isEmpty()) {
            return new CoreTimeline(List.of(), fallbackCores);
        }
        List<ExecutorEvent> sorted = new ArrayList<>(events);
        sorted.sort(Comparator.comparingLong(ExecutorEvent::timeMs));

        List<Segment> segs = new ArrayList<>();
        int running = 0;
        long prevTime = sorted.get(0).timeMs();
        for (ExecutorEvent e : sorted) {
            if (e.timeMs() > prevTime && running > 0) {
                segs.add(new Segment(prevTime, e.timeMs(), running));
            }
            running += e.added() ? e.cores() : -e.cores();
            if (running < 0) running = 0; // defensive against unmatched removes
            prevTime = e.timeMs();
        }
        // Trailing open segment is closed lazily in coreMillis() against the query window end.
        if (running > 0) {
            segs.add(new Segment(prevTime, Long.MAX_VALUE, running));
        }
        return new CoreTimeline(segs, fallbackCores);
    }

    /**
     * Capacity (core-milliseconds) available in the window [startMs, endMs).
     * Falls back to {@code fallbackCores * window} when no segments exist.
     */
    public long coreMillis(long startMs, long endMs) {
        if (endMs <= startMs) {
            return 0;
        }
        if (segments.isEmpty()) {
            return (long) fallbackCores * (endMs - startMs);
        }
        long total = 0;
        for (Segment s : segments) {
            long a = Math.max(s.startMs(), startMs);
            long b = Math.min(s.endMs() == Long.MAX_VALUE ? endMs : s.endMs(), endMs);
            if (b > a) {
                total += (long) s.cores() * (b - a);
            }
        }
        return total;
    }

    /** Peak concurrent cores observed (or fallback when no events). */
    public int peakCores() {
        return segments.isEmpty()
                ? fallbackCores
                : segments.stream().mapToInt(Segment::cores).max().orElse(fallbackCores);
    }

    public boolean hasData() {
        return !segments.isEmpty();
    }
}
