package io.sparkadvisor.core.model;

import java.util.List;
import java.util.Map;

/**
 * The fully parsed application: the in-memory result of replaying an event log.
 *
 * @param conf       relevant spark.* configuration captured from EnvironmentUpdate
 * @param incomplete true if the log was truncated / .inprogress / compacted, so some
 *                   entities may be missing end events; downstream must lower confidence
 * @param executorEvents executor add/remove events for accurate core-timeline reconstruction
 * @param taskIntervals optional lightweight task intervals for queue contention analysis; empty
 *                      in the default single-SQL low-memory path
 */
public record ApplicationModel(
        String appId,
        String appName,
        long startTime,
        long endTime,
        boolean incomplete,
        Map<String, String> conf,
        List<SqlExecution> sqlExecutions,
        List<Job> jobs,
        List<Stage> stages,
        List<ExecutorEvent> executorEvents,
        List<TaskInterval> taskIntervals) {

    public long wallClockMs() {
        if (startTime <= 0 || endTime <= 0) return 0;
        return endTime - startTime;
    }
}
