package io.sparkadvisor.core.analyze;

import java.util.List;

/**
 * Aggregated analytical view of a single SQL execution: its stages plus the three
 * reference timelines that frame the optimization headroom.
 *
 * <p>The three timelines (see design doc §7.2):
 * <ul>
 *   <li>{@code wallClockMs}     — actual observed duration</li>
 *   <li>{@code criticalPathMs}  — sum of per-stage max-task times along the longest
 *       dependency path; the lower bound under infinite executors</li>
 *   <li>{@code idealMs}         — same path with perfectly balanced tasks
 *       (totalTaskTime / availableCores per stage); the lower bound with zero skew</li>
 * </ul>
 * Relationship: {@code idealMs <= criticalPathMs <= wallClockMs}.
 *
 * @param deviation (wallClock - criticalPath) / criticalPath; distance from the
 *                  infinite-executor lower bound (0 if criticalPath is 0)
 */
public record SqlAnalysis(
        long executionId,
        String statementId,
        String description,
        String physicalPlanText,
        long wallClockMs,
        long criticalPathMs,
        long idealMs,
        double deviation,
        double coreUtilization,
        List<StageAnalysis> stages) {

    /** The stages sorted by wall-clock time, slowest first. */
    public List<StageAnalysis> stagesByDurationDesc() {
        return stages.stream()
                .sorted(java.util.Comparator.comparingLong(StageAnalysis::wallClockMs).reversed())
                .toList();
    }
}
