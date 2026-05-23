package io.sparkadvisor.core.model;

import java.util.List;

/**
 * A completed (or partially observed) stage attempt.
 *
 * @param schedulingDelayMs firstTaskLaunch - submissionTime; high values indicate
 *                          resource/scheduling wait (dynamic allocation cold start, etc.)
 */
public record Stage(
        int stageId,
        int attemptId,
        int numTasks,
        List<Integer> parentStageIds,
        long submissionTime,
        long firstTaskLaunchTime,
        long completionTime,
        long shuffleReadTotalBytes,
        long shuffleWriteTotalBytes,
        TaskMetricStats taskStats) {

    public long wallClockMs() {
        if (submissionTime <= 0 || completionTime <= 0) return 0;
        return completionTime - submissionTime;
    }

    public long schedulingDelayMs() {
        if (submissionTime <= 0 || firstTaskLaunchTime <= 0) return 0;
        return Math.max(0, firstTaskLaunchTime - submissionTime);
    }
}
