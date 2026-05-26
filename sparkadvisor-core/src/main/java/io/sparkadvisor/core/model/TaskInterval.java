package io.sparkadvisor.core.model;

import io.sparkadvisor.core.util.ValueObjects;

import java.util.Objects;

/**
 * Lightweight task interval retained only when queue-level contention analysis requests it.
 */
public final class TaskInterval {
    private final long taskId;
    private final int stageId;
    private final int stageAttemptId;
    private final Long sqlExecutionId;
    private final String executorId;
    private final long launchTime;
    private final long finishTime;

    public TaskInterval(long taskId, int stageId, int stageAttemptId, Long sqlExecutionId,
                        String executorId, long launchTime, long finishTime) {
        this.taskId = taskId;
        this.stageId = stageId;
        this.stageAttemptId = stageAttemptId;
        this.sqlExecutionId = sqlExecutionId;
        this.executorId = executorId;
        this.launchTime = launchTime;
        this.finishTime = finishTime;
    }

    public long taskId() { return taskId; }
    public int stageId() { return stageId; }
    public int stageAttemptId() { return stageAttemptId; }
    public Long sqlExecutionId() { return sqlExecutionId; }
    public String executorId() { return executorId; }
    public long launchTime() { return launchTime; }
    public long finishTime() { return finishTime; }

    public long durationMs() { return Math.max(0L, finishTime - launchTime); }

    @Override public boolean equals(Object o) { if (this == o) return true; if (!(o instanceof TaskInterval)) return false; TaskInterval that = (TaskInterval) o; return taskId == that.taskId && stageId == that.stageId && stageAttemptId == that.stageAttemptId && launchTime == that.launchTime && finishTime == that.finishTime && Objects.equals(sqlExecutionId, that.sqlExecutionId) && Objects.equals(executorId, that.executorId); }
    @Override public int hashCode() { return Objects.hash(taskId, stageId, stageAttemptId, sqlExecutionId, executorId, launchTime, finishTime); }
    @Override public String toString(){return ValueObjects.toString(this);}
}
