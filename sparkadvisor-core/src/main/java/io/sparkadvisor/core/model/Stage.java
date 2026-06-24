package io.sparkadvisor.core.model;

import io.sparkadvisor.core.util.ValueObjects;

import java.util.List;
import java.util.Objects;

public final class Stage {
    private final int stageId;
    private final int attemptId;
    private final int numTasks;
    private final List<Integer> parentStageIds;
    private final long submissionTime;
    private final long firstTaskLaunchTime;
    private final long completionTime;
    private final long shuffleReadTotalBytes;
    private final long shuffleWriteTotalBytes;
    private final long shuffleFetchWaitMs;
    private final long shuffleRemoteReadBytes;
    private final int failedTaskAttempts;
    private final int extraTaskAttempts;
    private final long maxTaskId;
    private final TaskMetricStats taskStats;

    public Stage(int stageId, int attemptId, int numTasks, List<Integer> parentStageIds, long submissionTime,
                 long firstTaskLaunchTime, long completionTime, long shuffleReadTotalBytes,
                 long shuffleWriteTotalBytes, TaskMetricStats taskStats) {
        this(stageId, attemptId, numTasks, parentStageIds, submissionTime, firstTaskLaunchTime,
                completionTime, shuffleReadTotalBytes, shuffleWriteTotalBytes,
                0L, 0L, 0, 0, -1L, taskStats);
    }

    public Stage(int stageId, int attemptId, int numTasks, List<Integer> parentStageIds, long submissionTime,
                 long firstTaskLaunchTime, long completionTime, long shuffleReadTotalBytes,
                 long shuffleWriteTotalBytes, long shuffleFetchWaitMs, long shuffleRemoteReadBytes,
                 int failedTaskAttempts, int extraTaskAttempts, TaskMetricStats taskStats) {
        this(stageId, attemptId, numTasks, parentStageIds, submissionTime, firstTaskLaunchTime, completionTime,
                shuffleReadTotalBytes, shuffleWriteTotalBytes, shuffleFetchWaitMs, shuffleRemoteReadBytes,
                failedTaskAttempts, extraTaskAttempts, -1L, taskStats);
    }

    public Stage(int stageId, int attemptId, int numTasks, List<Integer> parentStageIds, long submissionTime,
                 long firstTaskLaunchTime, long completionTime, long shuffleReadTotalBytes,
                 long shuffleWriteTotalBytes, long shuffleFetchWaitMs, long shuffleRemoteReadBytes,
                 int failedTaskAttempts, int extraTaskAttempts, long maxTaskId, TaskMetricStats taskStats) {
        this.stageId = stageId;
        this.attemptId = attemptId;
        this.numTasks = numTasks;
        this.parentStageIds = parentStageIds;
        this.submissionTime = submissionTime;
        this.firstTaskLaunchTime = firstTaskLaunchTime;
        this.completionTime = completionTime;
        this.shuffleReadTotalBytes = shuffleReadTotalBytes;
        this.shuffleWriteTotalBytes = shuffleWriteTotalBytes;
        this.shuffleFetchWaitMs = shuffleFetchWaitMs;
        this.shuffleRemoteReadBytes = shuffleRemoteReadBytes;
        this.failedTaskAttempts = failedTaskAttempts;
        this.extraTaskAttempts = extraTaskAttempts;
        this.maxTaskId = maxTaskId;
        this.taskStats = taskStats;
    }
    public int stageId() { return stageId; }
    public int attemptId() { return attemptId; }
    public int numTasks() { return numTasks; }
    public List<Integer> parentStageIds() { return parentStageIds; }
    public long submissionTime() { return submissionTime; }
    public long firstTaskLaunchTime() { return firstTaskLaunchTime; }
    public long completionTime() { return completionTime; }
    public long shuffleReadTotalBytes() { return shuffleReadTotalBytes; }
    public long shuffleWriteTotalBytes() { return shuffleWriteTotalBytes; }
    public long shuffleFetchWaitMs() { return shuffleFetchWaitMs; }
    public long shuffleRemoteReadBytes() { return shuffleRemoteReadBytes; }
    public int failedTaskAttempts() { return failedTaskAttempts; }
    public int extraTaskAttempts() { return extraTaskAttempts; }
    public long maxTaskId() { return maxTaskId; }
    public TaskMetricStats taskStats() { return taskStats; }
    public long wallClockMs() { return (submissionTime <= 0 || completionTime <= 0) ? 0 : completionTime - submissionTime; }
    public long schedulingDelayMs() { return (submissionTime <= 0 || firstTaskLaunchTime <= 0) ? 0 : Math.max(0, firstTaskLaunchTime - submissionTime); }
    @Override public boolean equals(Object o) { if (this == o) return true; if (!(o instanceof Stage)) return false; Stage stage = (Stage) o; return stageId == stage.stageId && attemptId == stage.attemptId && numTasks == stage.numTasks && submissionTime == stage.submissionTime && firstTaskLaunchTime == stage.firstTaskLaunchTime && completionTime == stage.completionTime && shuffleReadTotalBytes == stage.shuffleReadTotalBytes && shuffleWriteTotalBytes == stage.shuffleWriteTotalBytes && shuffleFetchWaitMs == stage.shuffleFetchWaitMs && shuffleRemoteReadBytes == stage.shuffleRemoteReadBytes && failedTaskAttempts == stage.failedTaskAttempts && extraTaskAttempts == stage.extraTaskAttempts && maxTaskId == stage.maxTaskId && Objects.equals(parentStageIds, stage.parentStageIds) && Objects.equals(taskStats, stage.taskStats); }
    @Override public int hashCode() { return Objects.hash(stageId, attemptId, numTasks, parentStageIds, submissionTime, firstTaskLaunchTime, completionTime, shuffleReadTotalBytes, shuffleWriteTotalBytes, shuffleFetchWaitMs, shuffleRemoteReadBytes, failedTaskAttempts, extraTaskAttempts, maxTaskId, taskStats); }
    @Override public String toString(){return ValueObjects.toString(this);}
}
