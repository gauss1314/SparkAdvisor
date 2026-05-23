package io.sparkadvisor.core.model;

import java.util.List;

/**
 * A Spark job. {@code sqlExecutionId} links it to a {@link SqlExecution} when the
 * job was launched from a SQL execution (read from the job's
 * {@code spark.sql.execution.id} property).
 */
public record Job(
        int jobId,
        Long sqlExecutionId,
        List<Integer> stageIds,
        long submissionTime,
        long completionTime) {

    public long wallClockMs() {
        if (submissionTime <= 0 || completionTime <= 0) return 0;
        return completionTime - submissionTime;
    }
}
