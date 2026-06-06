package io.sparkadvisor.monitor;

import io.sparkadvisor.core.metrics.Distribution;
import io.sparkadvisor.core.metrics.MetricDistributionBuilder;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.Job;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.core.model.Stage;
import io.sparkadvisor.core.model.TaskInterval;
import io.sparkadvisor.core.model.TaskMetricStats;
import io.sparkadvisor.monitor.advisor.QueueLlmAdvisor;
import io.sparkadvisor.monitor.render.QueueHtmlWriter;
import io.sparkadvisor.monitor.render.QueueJsonWriter;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueueAnalyzerTest {

    private static Distribution dist(long... values) {
        MetricDistributionBuilder b = new MetricDistributionBuilder();
        for (long value : values) {
            b.add(value);
        }
        return b.build();
    }

    private static Stage stage(int id, int tasks, long submit, long firstLaunch, long complete,
                               Distribution durations, Distribution shuffleRead) {
        TaskMetricStats stats = new TaskMetricStats(
                durations,
                shuffleRead,
                Distribution.EMPTY,
                Distribution.EMPTY,
                Distribution.EMPTY,
                Distribution.EMPTY,
                Distribution.EMPTY,
                Distribution.EMPTY,
                Distribution.EMPTY);
        return new Stage(id, 0, tasks, new java.util.ArrayList<>(), submit, firstLaunch, complete,
                shuffleRead.sum(), 0L, stats);
    }

    private static Stage stageWithStats(int id, int tasks, long submit, long firstLaunch, long complete,
                                        Distribution durations, Distribution shuffleRead,
                                        Distribution input, Distribution spill, Distribution gc) {
        return stageWithStats(id, tasks, submit, firstLaunch, complete, durations, shuffleRead,
                input, spill, gc, 0, 0);
    }

    private static Stage stageWithStats(int id, int tasks, long submit, long firstLaunch, long complete,
                                        Distribution durations, Distribution shuffleRead,
                                        Distribution input, Distribution spill, Distribution gc,
                                        int failedAttempts, int extraAttempts) {
        TaskMetricStats stats = new TaskMetricStats(
                durations,
                shuffleRead,
                Distribution.EMPTY,
                input,
                Distribution.EMPTY,
                spill,
                Distribution.EMPTY,
                gc,
                Distribution.EMPTY);
        return new Stage(id, 0, tasks, new java.util.ArrayList<>(), submit, firstLaunch, complete,
                shuffleRead.sum(), 0L, 0L, 0L, failedAttempts, extraAttempts, stats);
    }

    private static ApplicationModel queueApp() {
        SqlExecution hog = new SqlExecution(1L, "big",
                "/* big */ select * from large_join", "", 1L, 60_000L, false, java.util.Arrays.asList(1L));
        SqlExecution skewed = new SqlExecution(2L, "small",
                "/* small */ select * from skewed_join", "", 10_000L, 60_000L, false, java.util.Arrays.asList(2L));
        Job job1 = new Job(1, 1L, java.util.Arrays.asList(1), 1L, 60_000L, false);
        Job job2 = new Job(2, 2L, java.util.Arrays.asList(2), 10_000L, 60_000L, false);
        Stage hogStage = stage(1, 3, 1L, 1L, 60_000L,
                dist(60_000, 60_000, 60_000),
                dist(1024L, 1024L, 1024L));
        Stage skewStage = stage(2, 10, 10_000L, 10_000L, 60_000L,
                dist(500, 500, 500, 500, 500, 500, 500, 500, 500, 50_000),
                dist(1_000_000L, 1_000_000L, 50_000_000L));
        Map<String, String> conf = new LinkedHashMap<>();
        conf.put("spark.executor.instances", "2");
        conf.put("spark.executor.cores", "2");
        return new ApplicationModel("app-queue", "Queue", 1L, 60_000L, false, conf,
                java.util.Arrays.asList(hog, skewed),
                java.util.Arrays.asList(job1, job2),
                java.util.Arrays.asList(hogStage, skewStage),
	                new java.util.ArrayList<>(),
	                java.util.Arrays.asList(
	                        new TaskInterval(1, 1, 0, 1L, "1", 1L, 60_000L,
	                                59_999L, 45_000_000_000L, 100L, 0L, false, false),
	                        new TaskInterval(2, 1, 0, 1L, "2", 1L, 60_000L,
	                                59_999L, 44_000_000_000L, 100L, 0L, false, false),
	                        new TaskInterval(3, 1, 0, 1L, "3", 1L, 60_000L,
	                                59_999L, 43_000_000_000L, 100L, 0L, false, false),
	                        new TaskInterval(4, 2, 0, 2L, "4", 10_000L, 60_000L,
	                                50_000L, 35_000_000_000L, 100L, 0L, false, false),
	                        new TaskInterval(5, 2, 0, 2L, "4", 10_000L, 10_500L,
	                                500L, 350_000_000L, 0L, 0L, false, false)));
    }

    private static ApplicationModel lightMetricQueueApp() {
        java.util.List<SqlExecution> sqls = new java.util.ArrayList<>();
        java.util.List<Job> jobs = new java.util.ArrayList<>();
        java.util.List<Stage> stages = new java.util.ArrayList<>();

        sqls.add(new SqlExecution(1L, "slow",
                "/* slow */ select count(*) from large_table", "", 1L, 120_001L, false,
                java.util.Arrays.asList(1L)));
        jobs.add(new Job(1, 1L, java.util.Arrays.asList(1), 1L, 120_001L, false));
        stages.add(stageWithStats(1, 10, 1L, 1L, 120_001L,
                new Distribution(10, 12_000L, 12_000L, 12_000L, 12_000L, 12_000L, 12_000L, 120_000L),
                Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY));

        for (int i = 2; i <= 6; i++) {
            long start = i * 30_000L;
            long end = start + 20_000L;
            sqls.add(new SqlExecution(i, "small-" + i,
                    "/* small_" + i + " */ select * from tiny_files where id = " + i,
                    "Scan parquet tiny_files", start, end, false,
                    java.util.Arrays.asList((long) i)));
            jobs.add(new Job(i, (long) i, java.util.Arrays.asList(i), start, end, false));
            stages.add(stageWithStats(i, 2_500, start, start, end,
                    new Distribution(2_500, 100L, 100L, 100L, 100L, 100L, 100L, 250_000L),
                    Distribution.EMPTY,
                    new Distribution(2_500, 1_024L, 1_024L, 1_024L, 1_024L, 1_024L, 1_024L, 2_560_000L),
                    Distribution.EMPTY,
                    Distribution.EMPTY));
        }

        Map<String, String> conf = new LinkedHashMap<>();
        conf.put("spark.executor.instances", "2");
        conf.put("spark.executor.cores", "2");
        return new ApplicationModel("app-light", "LightMetricQueue", 1L, 200_000L, false, conf,
                sqls, jobs, stages, new java.util.ArrayList<>(), new java.util.ArrayList<>());
    }

    private static ApplicationModel repeatedLargeScanQueueApp() {
        java.util.List<SqlExecution> sqls = new java.util.ArrayList<>();
        java.util.List<Job> jobs = new java.util.ArrayList<>();
        java.util.List<Stage> stages = new java.util.ArrayList<>();
        long inputPerQuery = 256L * 1024L * 1024L;
        for (int i = 1; i <= 3; i++) {
            long start = i * 20_000L;
            long end = start + 15_000L;
            sqls.add(new SqlExecution(i, "scan-" + i,
                    "/* scan_" + i + " */ select sum(v) from fact where ds = " + i,
                    "Scan parquet fact", start, end, false,
                    java.util.Arrays.asList((long) i)));
            jobs.add(new Job(i, (long) i, java.util.Arrays.asList(i), start, end, false));
            stages.add(stageWithStats(i, 64, start, start, end,
                    new Distribution(64, 200L, 200L, 200L, 200L, 200L, 200L, 12_800L),
                    Distribution.EMPTY,
                    new Distribution(64, 4L * 1024L * 1024L, 4L * 1024L * 1024L,
                            4L * 1024L * 1024L, 4L * 1024L * 1024L,
                            4L * 1024L * 1024L, 4L * 1024L * 1024L, inputPerQuery),
                    Distribution.EMPTY,
                    Distribution.EMPTY));
        }
        Map<String, String> conf = new LinkedHashMap<>();
        conf.put("spark.executor.instances", "2");
        conf.put("spark.executor.cores", "2");
        return new ApplicationModel("app-scan", "RepeatedScanQueue", 1L, 100_000L, false, conf,
                sqls, jobs, stages, new java.util.ArrayList<>(), new java.util.ArrayList<>());
    }

    private static ApplicationModel schedulingAndAttemptQueueApp() {
        java.util.List<SqlExecution> sqls = new java.util.ArrayList<>();
        java.util.List<Job> jobs = new java.util.ArrayList<>();
        java.util.List<Stage> stages = new java.util.ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            long start = i * 15_000L;
            long firstLaunch = start + 5_000L;
            long end = start + 10_000L;
            sqls.add(new SqlExecution(i, "retry-" + i,
                    "/* retry_" + i + " */ select count(*) from unstable where id = " + i,
                    "", start, end, false, java.util.Arrays.asList((long) i)));
            jobs.add(new Job(i, (long) i, java.util.Arrays.asList(i), start, end, false));
            stages.add(stageWithStats(i, 10, start, firstLaunch, end,
                    new Distribution(10, 500L, 500L, 500L, 500L, 500L, 500L, 5_000L),
                    Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY,
                    1, 0));
        }
        Map<String, String> conf = new LinkedHashMap<>();
        conf.put("spark.executor.instances", "1");
        conf.put("spark.executor.cores", "2");
        return new ApplicationModel("app-attempts", "SchedulingAttempts", 1L, 100_000L, false, conf,
                sqls, jobs, stages, new java.util.ArrayList<>(), new java.util.ArrayList<>());
    }

    @Test
    void buildsQueueContractWithContentionAndBottlenecks() {
        var result = new QueueAnalyzer().analyze(queueApp(), "synthetic", 2, 10_000L);

        assertEquals("app-queue", result.summary().appId());
        assertEquals(2, result.summary().completedQueries());
        assertEquals(4, result.summary().fixedExecutorCores());
        assertTrue(result.utilization().avgUtilization() > 0.80, "pool should be busy");
        assertTrue(result.resources().avgCpuEfficiency() > 0.50, "CPU efficiency should come from task intervals");
        assertTrue(result.contention().contentionLimitedPct() > 0.0, "one query should be contention-limited");
        assertTrue(result.topSlowQueries().stream().anyMatch(q -> q.executionId() == 2L
                && q.contentionLimited()));
        assertTrue(result.bottlenecks().stream().anyMatch(b -> b.ruleId().equals("R1_DATA_SKEW")));
        assertNotNull(result.resources());
        assertTrue(result.meta().deepCoveragePct() > 0.0);
        assertTrue(result.globalRecommendations().stream()
                .anyMatch(r -> r.queueRuleId().equals("Q2_COMMON_LONG_TAIL_SKEW")));
    }

    @Test
    void rendersHtmlAndJsonContracts() throws Exception {
        var result = new QueueAnalyzer().analyze(queueApp(), "synthetic", 2, 10_000L);
        String html = new QueueHtmlWriter().render(result);
        String json = new QueueJsonWriter().toJson(result);

        assertTrue(html.startsWith("<!DOCTYPE html>"));
        assertTrue(html.contains("SparkAdvisor Queue"));
        assertTrue(html.contains("Global recommendations"));
        assertTrue(html.contains("<svg class=\"chart\""), "timeline chart should be inline SVG");
        assertTrue(html.contains("?statementId=big"), "slow-query rows should link to drilldown");
        assertTrue(html.contains("Template cost"));
        assertTrue(json.contains("\"summary\""));
        assertTrue(json.contains("\"resources\""));
        assertTrue(json.contains("\"bottlenecks\""));
        assertTrue(json.contains("\"templateStats\""));
        assertTrue(json.contains("\"deepCoveragePct\""));
        assertNotNull(result.meta().assumptions());
    }

    @Test
    void deepAnalysisAddsRepresentativeStrataBeyondTopN() {
        var result = new QueueAnalyzer().analyze(queueApp(), "synthetic", 1, 5, 10_000L);

        assertTrue(result.sampledQueries().stream().anyMatch(q -> q.executionId() == 2L),
                "skew-heavy query should be selected by stratum even when it is not top-1");
        assertTrue(result.meta().deepAnalyzedQueries() > 1);
    }

    @Test
    void queueRulesUseFullLightMetricsWhenDeepSampleMissesCommonSignals() {
        var result = new QueueAnalyzer().analyze(lightMetricQueueApp(), "synthetic-light", 1, 0, 60_000L);

        assertEquals(1, result.meta().deepAnalyzedQueries());
        assertTrue(result.templateStats().stream()
                        .anyMatch(t -> t.queryCount() == 5 && t.totalInputBytes() > 0L),
                "template stats should aggregate repeated normalized SQL text");
        assertTrue(result.bottlenecks().stream()
                        .anyMatch(b -> b.ruleId().equals("R5_SMALL_FILES")
                                && b.scope().contains("FULL_QUEUE")
                                && b.affectedQueries() == 5
                                && b.affectedPct() > 0.80),
                "small-file cluster should be derived from all completed SQL light metrics");
        assertTrue(result.globalRecommendations().stream()
                        .anyMatch(r -> r.queueRuleId().equals("Q7_COMMON_SMALL_FILES")
                                && r.caveats().contains("Light metrics cover all completed SQL executions")),
                "Q7 should trigger even though the small-file SQLs were not deep-analyzed");
        assertTrue(result.globalRecommendations().stream()
                        .anyMatch(r -> r.queueRuleId().equals("Q14_HIGH_FREQUENCY_TEMPLATE_COST")),
                "Q14 should prioritize repeated template cost");
    }

    @Test
    void queueRulesDetectRepeatedLargeScanTemplates() {
        var result = new QueueAnalyzer().analyze(repeatedLargeScanQueueApp(), "synthetic-scan", 1, 0, 60_000L);

        assertTrue(result.templateStats().stream()
                        .anyMatch(t -> t.queryCount() == 3
                                && t.totalInputBytes() >= 768L * 1024L * 1024L),
                "template stats should retain repeated large scan input bytes");
        assertTrue(result.globalRecommendations().stream()
                        .anyMatch(r -> r.queueRuleId().equals("Q15_REPEATED_LARGE_SCAN_CACHE_OR_MATERIALIZE")),
                "Q15 should trigger for repeated large scans");
    }

    @Test
    void queueRulesDetectSchedulingDelayAndAttemptNoise() {
        var result = new QueueAnalyzer().analyze(schedulingAndAttemptQueueApp(),
                "synthetic-attempts", 1, 0, 60_000L);

        assertTrue(result.bottlenecks().stream()
                        .anyMatch(b -> b.ruleId().equals("R8_SCHEDULING_DELAY")
                                && b.scope().contains("FULL_QUEUE")),
                "R8 queue cluster should come from full light metrics");
        assertTrue(result.bottlenecks().stream()
                        .anyMatch(b -> b.ruleId().equals("R10_TASK_RETRY")
                                && b.scope().contains("FULL_QUEUE")),
                "R10 queue cluster should come from full light metrics");
        assertTrue(result.globalRecommendations().stream()
                        .anyMatch(r -> r.queueRuleId().equals("Q12_SCHEDULING_COLD_START_OR_POOL_DELAY")),
                "Q12 should trigger for repeated scheduling delay");
        assertTrue(result.globalRecommendations().stream()
                        .anyMatch(r -> r.queueRuleId().equals("Q13_QUEUE_RETRY_OR_SPECULATION_NOISE")),
                "Q13 should trigger for repeated failed attempts");
    }

    @Test
    void queueMetaUsesCallerSnapshotAndDegradedReason() throws Exception {
        var result = new QueueAnalyzer().analyze(queueApp(), "synthetic", 2, 5, 10_000L,
                QueueAnalysisContext.fullSnapshot("snapshot-123", "full replay fallback"));
        String html = new QueueHtmlWriter().render(result);

        assertEquals("snapshot-123", result.meta().snapshotKey());
        assertFalse(result.meta().incremental());
        assertEquals("full replay fallback", result.meta().degradedReason());
        assertTrue(html.contains("full replay fallback"));
    }

    @Test
    void queueJsonRedactsSensitiveMetadata() throws Exception {
        var result = new QueueAnalyzer().analyze(queueApp(),
                "hdfs://namenode.example.com/user/alice/eventLog?password=secret", 2, 5, 10_000L,
                QueueAnalysisContext.fullSnapshot(
                        "hdfs://namenode.example.com/user/alice/eventLog|token=abc",
                        "checkpoint used token=abc"));
        String json = new QueueJsonWriter().toJson(result);

        assertTrue(json.contains("hdfs://<redacted-authority>"));
        assertTrue(json.contains("password=<redacted>"));
        assertTrue(json.contains("token=<redacted>"));
    }

    @Test
    void rendersChineseQueueReport() throws Exception {
        var result = new QueueAnalyzer().analyze(queueApp(), "synthetic", 2, 10_000L);
        String html = new QueueHtmlWriter().render(result, true);
        assertTrue(html.contains("lang=\"zh-CN\""));
        assertTrue(html.contains("队列概览"));
        assertTrue(html.contains("全局调参建议"));
    }

    @Test
    void queueLlmAdvisorParsesStructuredAdvice() {
        var result = new QueueAnalyzer().analyze(queueApp(), "synthetic", 2, 10_000L);
        var advisor = new QueueLlmAdvisor(new io.sparkadvisor.advisor.llm.LlmProvider() {
            public String name() { return "llm:test"; }
            public String complete(String systemPrompt, String userPrompt) {
                assertTrue(userPrompt.contains("\"topSlowQueries\""));
                return "{\"summary\":\"Queue is contention-limited.\",\"recommendations\":["
                        + "{\"type\":\"SPARK_CONF\",\"action\":\"increase executor pool\","
                        + "\"rationale\":\"high contention\",\"expectedImpact\":\"medium\"}]}";
            }
        });
        var advice = advisor.advise(result);
        var withAdvice = result.withAiAdvice(advice);
        assertEquals("llm:test", withAdvice.aiAdvice().provider());
        assertEquals(1, withAdvice.aiAdvice().recommendations().size());
    }
}
