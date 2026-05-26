package io.sparkadvisor.core.eventlog;

import io.sparkadvisor.core.locate.StatementIdExtractor;
import io.sparkadvisor.core.metrics.MetricDistributionBuilder;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.Job;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.core.model.Stage;
import io.sparkadvisor.core.model.TaskInterval;
import io.sparkadvisor.core.model.TaskMetricStats;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

import io.sparkadvisor.core.model.ExecutorEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A custom {@link SparkListener} (written in Java) that ReplayListenerBus feeds events
 * into. It accumulates everything SparkAdvisor needs into Java-typed builders and then
 * materializes an immutable {@link ApplicationModel}.
 *
 * <h2>Design notes</h2>
 * <ul>
 *   <li><b>SQL events</b> (SparkListenerSQLExecutionStart/End, AQE updates) are NOT
 *       delivered through named callbacks; they arrive via {@link #onOtherEvent}. We keep
 *       a thin coupling to spark-sql types there. See {@code // VERIFY@3.5.1} markers.</li>
 *   <li><b>Thrift Server events</b> are matched by class name reflectively so we never
 *       hard-depend on hive-thriftserver being present.</li>
 *   <li><b>Memory</b>: per-stage metrics go straight into {@link MetricDistributionBuilder};
 *       individual tasks are never retained.</li>
 *   <li><b>Scala interop</b>: any Scala collections/Options returned by Spark are converted
 *       to Java types here so the rest of core/analyzer never sees Scala.</li>
 * </ul>
 *
 * <p>Not thread-safe; replay is single-threaded per bus.
 */
public final class SparkEventCollector extends SparkListener {

    private static final Logger LOG = Logger.getLogger(SparkEventCollector.class.getName());

    private static final String SQL_EXEC_START =
            "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart";
    private static final String SQL_EXEC_END =
            "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd";
    private static final String THRIFT_OP_START =
            "org.apache.spark.sql.hive.thriftserver.SparkListenerThriftServerOperationStart";

    private final StatementIdExtractor statementIdExtractor = new StatementIdExtractor();
    private final boolean collectTaskIntervals;

    private String appId = "";
    private String appName = "";
    private long appStart = 0L;
    private long appEnd = 0L;
    private boolean incomplete = true; // set false once we see ApplicationEnd
    private final Map<String, String> conf = new LinkedHashMap<>();

    private final Map<Long, SqlExecBuilder> sqlExecs = new LinkedHashMap<>();
    private final List<Job> jobs = new ArrayList<>();
    private final Map<Integer, StageBuilder> stages = new LinkedHashMap<>();
    private final Map<Integer, Long> stageSqlExecutions = new HashMap<>();
    private final List<TaskInterval> taskIntervals = new ArrayList<>();

    private final List<ExecutorEvent> executorEvents = new ArrayList<>();
    private final Map<String, Integer> executorCores = new HashMap<>(); // executorId -> cores

    public SparkEventCollector() {
        this(false);
    }

    public SparkEventCollector(boolean collectTaskIntervals) {
        this.collectTaskIntervals = collectTaskIntervals;
    }

    // ---- Application lifecycle -------------------------------------------------

    @Override
    public void onApplicationStart(SparkListenerApplicationStart e) {
        this.appName = e.appName();
        this.appStart = e.time();
        // appId is Option[String] in Scala; convert defensively.
        if (e.appId().isDefined()) {
            this.appId = e.appId().get();
        }
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd e) {
        this.appEnd = e.time();
        this.incomplete = false;
    }

    @Override
    public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate e) {
        // e.environmentDetails(): scala.collection.Map[String, Seq[(String,String)]]
        // We only need "Spark Properties". Convert via the interop helper.
        ScalaInterop.sparkProperties(e).forEach(conf::putIfAbsent);
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded e) {
        // VERIFY@3.5.1: e.time():long, e.executorId():String, e.executorInfo().totalCores():int
        int cores = e.executorInfo().totalCores();
        executorCores.put(e.executorId(), cores);
        executorEvents.add(new ExecutorEvent(e.time(), cores, true));
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved e) {
        // VERIFY@3.5.1: e.time():long, e.executorId():String
        Integer cores = executorCores.remove(e.executorId());
        if (cores != null) {
            executorEvents.add(new ExecutorEvent(e.time(), cores, false));
        }
    }

    // ---- Jobs / Stages / Tasks -------------------------------------------------

    @Override
    public void onJobStart(SparkListenerJobStart e) {
        // sqlExecutionId is carried in job properties under "spark.sql.execution.id"
        Long sqlId = ScalaInterop.sqlExecutionId(e.properties());
        // stageIds: Scala Seq[Object]; convert to Java List<Integer>
        List<Integer> stageIds = ScalaInterop.intSeq(e.stageIds());
        jobs.add(new Job(e.jobId(), sqlId, stageIds, e.time(), 0L, false));
        if (sqlId != null) {
            sqlExecs.computeIfAbsent(sqlId, SqlExecBuilder::new).jobIds.add((long) e.jobId());
            for (Integer stageId : stageIds) {
                stageSqlExecutions.putIfAbsent(stageId, sqlId);
            }
        }
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd e) {
        for (int i = 0; i < jobs.size(); i++) {
            Job j = jobs.get(i);
            if (j.jobId() == e.jobId() && j.completionTime() == 0L) {
                boolean failed = !e.jobResult().getClass().getName().contains("JobSucceeded");
                jobs.set(i, new Job(j.jobId(), j.sqlExecutionId(), j.stageIds(),
                        j.submissionTime(), e.time(), failed));
                break;
            }
        }
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted e) {
        var info = e.stageInfo();
        StageBuilder b = stages.computeIfAbsent(info.stageId(), id -> new StageBuilder());
        b.stageId = info.stageId();
        b.attemptId = info.attemptNumber();              // VERIFY@3.5.1 (attemptNumber vs attemptId)
        b.numTasks = info.numTasks();
        b.parentStageIds = ScalaInterop.intSeq(info.parentIds());
        // submissionTime is Option[Long]
        b.submissionTime = ScalaInterop.optLong(info.submissionTime());
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted e) {
        var info = e.stageInfo();
        StageBuilder b = stages.computeIfAbsent(info.stageId(), id -> new StageBuilder());
        b.stageId = info.stageId();
        b.numTasks = info.numTasks();
        b.completionTime = ScalaInterop.optLong(info.completionTime());
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd e) {
        StageBuilder b = stages.computeIfAbsent(e.stageId(), id -> new StageBuilder());
        // Track earliest task launch for scheduling-delay computation.
        long launch = e.taskInfo().launchTime();
        if (b.firstTaskLaunch == 0L || launch < b.firstTaskLaunch) {
            b.firstTaskLaunch = launch;
        }
        if (collectTaskIntervals) {
            long finish = e.taskInfo().finishTime(); // VERIFY@3.5.1
            if (finish > 0L && launch > 0L && finish >= launch) {
                taskIntervals.add(new TaskInterval(
                        e.taskInfo().taskId(),
                        e.stageId(),
                        e.stageAttemptId(),
                        stageSqlExecutions.get(e.stageId()),
                        e.taskInfo().executorId(),
                        launch,
                        finish));
            }
        }
        var m = e.taskMetrics();
        if (m == null) {
            return; // failed/speculative task without metrics
        }
        // Pull raw metrics; field accessors verified against Spark 3.5.1 TaskMetrics.
        b.duration.add(m.executorRunTime());                       // VERIFY@3.5.1
        b.gc.add(m.jvmGCTime());                                   // VERIFY@3.5.1
        b.deserialize.add(m.executorDeserializeTime());            // VERIFY@3.5.1
        b.memorySpill.add(m.memoryBytesSpilled());
        b.diskSpill.add(m.diskBytesSpilled());
        b.input.add(m.inputMetrics().bytesRead());
        b.output.add(m.outputMetrics().bytesWritten());
        b.shuffleRead.add(m.shuffleReadMetrics().totalBytesRead()); // VERIFY@3.5.1
        b.shuffleWrite.add(m.shuffleWriteMetrics().bytesWritten()); // VERIFY@3.5.1
    }

    // ---- SQL + Thrift events (arrive via onOtherEvent) -------------------------

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        String cls = event.getClass().getName();
        switch (cls) {
            case SQL_EXEC_START:
                handleSqlStart(event);
                break;
            case SQL_EXEC_END:
                handleSqlEnd(event);
                break;
            case THRIFT_OP_START:
                handleThriftOpStart(event);
                break;
            default:
                break;
        }
    }

    private void handleSqlStart(SparkListenerEvent event) {
        // Accessed via the spark-sql type. VERIFY@3.5.1 field names:
        // executionId:Long, description:String, physicalPlanDescription:String, time:Long
        var s = SqlEventAccess.sqlExecutionStart(event);
        SqlExecBuilder b = sqlExecs.computeIfAbsent(s.executionId(), SqlExecBuilder::new);
        b.description = s.description();
        b.physicalPlanText = s.physicalPlanDescription();
        b.startTime = s.time();
        statementIdExtractor.extract(s.description()).ifPresent(id -> b.statementId = id);
    }

    private void handleSqlEnd(SparkListenerEvent event) {
        var s = SqlEventAccess.sqlExecutionEnd(event);
        SqlExecBuilder b = sqlExecs.computeIfAbsent(s.executionId(), SqlExecBuilder::new);
        b.endTime = s.time();
    }

    private void handleThriftOpStart(SparkListenerEvent event) {
        // Supplementary StatementID source. Reflective access keeps hive-thriftserver optional.
        SqlEventAccess.thriftStatement(event).ifPresent(stmt ->
                statementIdExtractor.extract(stmt).ifPresent(id ->
                        // Attach to the most recent SQL exec lacking a statementId, if any.
                        attachThriftStatementId(id)));
    }

    private void attachThriftStatementId(String id) {
        // Best-effort: STS operation start typically precedes the SQL execution; we keep the
        // id and let the SQLExecutionStart.description take precedence when present.
        for (SqlExecBuilder b : sqlExecs.values()) {
            if (b.statementId == null) {
                b.statementId = id;
                return;
            }
        }
        LOG.fine(() -> "Thrift StatementID seen but no pending SQL exec to attach: " + id);
    }

    // ---- Materialization -------------------------------------------------------

    public ApplicationModel build() {
        List<SqlExecution> execList = new ArrayList<>();
        for (SqlExecBuilder b : sqlExecs.values()) {
            boolean execIncomplete = b.startTime == 0L || b.endTime == 0L;
            execList.add(new SqlExecution(
                    b.executionId, b.statementId, b.description, b.physicalPlanText,
                    b.startTime, b.endTime, execIncomplete, new ArrayList<>(b.jobIds)));
        }
        List<Stage> stageList = new ArrayList<>();
        for (StageBuilder b : stages.values()) {
            stageList.add(b.toStage());
        }
        return new ApplicationModel(
                appId, appName, appStart, appEnd, incomplete,
                new LinkedHashMap<>(conf), new ArrayList<>(execList), new ArrayList<>(jobs), new ArrayList<>(stageList),
                new ArrayList<>(executorEvents), new ArrayList<>(taskIntervals));
    }

    // ---- Mutable builders ------------------------------------------------------

    private static final class SqlExecBuilder {
        final long executionId;
        String statementId;
        String description = "";
        String physicalPlanText = "";
        long startTime = 0L;
        long endTime = 0L;
        final List<Long> jobIds = new ArrayList<>();

        SqlExecBuilder(long executionId) {
            this.executionId = executionId;
        }
    }

    private static final class StageBuilder {
        int stageId;
        int attemptId;
        int numTasks;
        List<Integer> parentStageIds = new ArrayList<Integer>();
        long submissionTime = 0L;
        long firstTaskLaunch = 0L;
        long completionTime = 0L;

        final MetricDistributionBuilder duration = new MetricDistributionBuilder();
        final MetricDistributionBuilder shuffleRead = new MetricDistributionBuilder();
        final MetricDistributionBuilder shuffleWrite = new MetricDistributionBuilder();
        final MetricDistributionBuilder input = new MetricDistributionBuilder();
        final MetricDistributionBuilder output = new MetricDistributionBuilder();
        final MetricDistributionBuilder memorySpill = new MetricDistributionBuilder();
        final MetricDistributionBuilder diskSpill = new MetricDistributionBuilder();
        final MetricDistributionBuilder gc = new MetricDistributionBuilder();
        final MetricDistributionBuilder deserialize = new MetricDistributionBuilder();

        Stage toStage() {
            TaskMetricStats stats = new TaskMetricStats(
                    duration.build(), shuffleRead.build(), shuffleWrite.build(),
                    input.build(), output.build(), memorySpill.build(),
                    diskSpill.build(), gc.build(), deserialize.build());
            return new Stage(
                    stageId, attemptId, numTasks, parentStageIds,
                    submissionTime, firstTaskLaunch, completionTime,
                    shuffleRead.build().sum(), shuffleWrite.build().sum(), stats);
        }
    }
}
