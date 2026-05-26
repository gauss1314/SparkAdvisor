package io.sparkadvisor.core.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ApplicationModel {
    private final String appId;
    private final String appName;
    private final long startTime;
    private final long endTime;
    private final boolean incomplete;
    private final Map<String, String> conf;
    private final List<SqlExecution> sqlExecutions;
    private final List<Job> jobs;
    private final List<Stage> stages;
    private final List<ExecutorEvent> executorEvents;
    private final List<TaskInterval> taskIntervals;

    public ApplicationModel(String appId, String appName, long startTime, long endTime, boolean incomplete,
                            Map<String, String> conf, List<SqlExecution> sqlExecutions, List<Job> jobs,
                            List<Stage> stages, List<ExecutorEvent> executorEvents, List<TaskInterval> taskIntervals) {
        this.appId = appId;
        this.appName = appName;
        this.startTime = startTime;
        this.endTime = endTime;
        this.incomplete = incomplete;
        this.conf = conf;
        this.sqlExecutions = sqlExecutions;
        this.jobs = jobs;
        this.stages = stages;
        this.executorEvents = executorEvents;
        this.taskIntervals = taskIntervals;
    }

    public String appId() { return appId; }
    public String appName() { return appName; }
    public long startTime() { return startTime; }
    public long endTime() { return endTime; }
    public boolean incomplete() { return incomplete; }
    public Map<String, String> conf() { return conf; }
    public List<SqlExecution> sqlExecutions() { return sqlExecutions; }
    public List<Job> jobs() { return jobs; }
    public List<Stage> stages() { return stages; }
    public List<ExecutorEvent> executorEvents() { return executorEvents; }
    public List<TaskInterval> taskIntervals() { return taskIntervals; }
    public long wallClockMs() { return (startTime <= 0 || endTime <= 0) ? 0 : endTime - startTime; }

    @Override public boolean equals(Object o) { if (this == o) return true; if (!(o instanceof ApplicationModel)) return false; ApplicationModel that = (ApplicationModel) o; return startTime == that.startTime && endTime == that.endTime && incomplete == that.incomplete && Objects.equals(appId, that.appId) && Objects.equals(appName, that.appName) && Objects.equals(conf, that.conf) && Objects.equals(sqlExecutions, that.sqlExecutions) && Objects.equals(jobs, that.jobs) && Objects.equals(stages, that.stages) && Objects.equals(executorEvents, that.executorEvents) && Objects.equals(taskIntervals, that.taskIntervals); }
    @Override public int hashCode() { return Objects.hash(appId, appName, startTime, endTime, incomplete, conf, sqlExecutions, jobs, stages, executorEvents, taskIntervals); }
}
