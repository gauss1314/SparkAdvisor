package io.sparkadvisor.core.model;

/**
 * Lightweight task interval retained only when queue-level contention analysis requests it.
 *
 * <p>The default single-SQL path does not collect these records, preserving the low-memory
 * behavior required for GB-scale logs. The monitor module enables this option so it can build
 * a shared resource-contention timeline.
 *
 * @param sqlExecutionId SQL execution id inferred from job/stage linkage, or null when unknown
 */
public record TaskInterval(
        long taskId,
        int stageId,
        int stageAttemptId,
        Long sqlExecutionId,
        String executorId,
        long launchTime,
        long finishTime) {

    public long durationMs() {
        return Math.max(0L, finishTime - launchTime);
    }
}
