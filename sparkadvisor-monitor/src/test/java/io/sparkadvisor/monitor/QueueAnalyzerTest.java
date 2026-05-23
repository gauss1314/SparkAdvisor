package io.sparkadvisor.monitor;

import io.sparkadvisor.core.metrics.Distribution;
import io.sparkadvisor.core.metrics.MetricDistributionBuilder;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.Job;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.core.model.Stage;
import io.sparkadvisor.core.model.TaskInterval;
import io.sparkadvisor.core.model.TaskMetricStats;
import io.sparkadvisor.monitor.render.QueueHtmlWriter;
import io.sparkadvisor.monitor.render.QueueJsonWriter;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        return new Stage(id, 0, tasks, List.of(), submit, firstLaunch, complete,
                shuffleRead.sum(), 0L, stats);
    }

    private static ApplicationModel queueApp() {
        SqlExecution hog = new SqlExecution(1L, "big",
                "/* big */ select * from large_join", "", 1L, 60_000L, false, List.of(1L));
        SqlExecution skewed = new SqlExecution(2L, "small",
                "/* small */ select * from skewed_join", "", 10_000L, 60_000L, false, List.of(2L));
        Job job1 = new Job(1, 1L, List.of(1), 1L, 60_000L, false);
        Job job2 = new Job(2, 2L, List.of(2), 10_000L, 60_000L, false);
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
                List.of(hog, skewed),
                List.of(job1, job2),
                List.of(hogStage, skewStage),
                List.of(),
                List.of(
                        new TaskInterval(1, 1, 0, 1L, "1", 1L, 60_000L),
                        new TaskInterval(2, 1, 0, 1L, "2", 1L, 60_000L),
                        new TaskInterval(3, 1, 0, 1L, "3", 1L, 60_000L),
                        new TaskInterval(4, 2, 0, 2L, "4", 10_000L, 60_000L),
                        new TaskInterval(5, 2, 0, 2L, "4", 10_000L, 10_500L)));
    }

    @Test
    void buildsQueueContractWithContentionAndBottlenecks() {
        var result = new QueueAnalyzer().analyze(queueApp(), "synthetic", 2, 10_000L);

        assertEquals("app-queue", result.summary().appId());
        assertEquals(2, result.summary().completedQueries());
        assertEquals(4, result.summary().fixedExecutorCores());
        assertTrue(result.utilization().avgUtilization() > 0.80, "pool should be busy");
        assertTrue(result.contention().contentionLimitedPct() > 0.0, "one query should be contention-limited");
        assertTrue(result.topSlowQueries().stream().anyMatch(q -> q.executionId() == 2L
                && q.contentionLimited()));
        assertTrue(result.bottlenecks().stream().anyMatch(b -> b.ruleId().equals("R1_DATA_SKEW")));
        assertNotNull(result.resources());
        assertTrue(result.globalRecommendations().stream()
                .anyMatch(r -> r.queueRuleId().equals("Q2_COMMON_SKEW")));
    }

    @Test
    void rendersHtmlAndJsonContracts() throws Exception {
        var result = new QueueAnalyzer().analyze(queueApp(), "synthetic", 2, 10_000L);
        String html = new QueueHtmlWriter().render(result);
        String json = new QueueJsonWriter().toJson(result);

        assertTrue(html.startsWith("<!DOCTYPE html>"));
        assertTrue(html.contains("SparkAdvisor Queue"));
        assertTrue(html.contains("Global recommendations"));
        assertTrue(json.contains("\"summary\""));
        assertTrue(json.contains("\"resources\""));
        assertTrue(json.contains("\"bottlenecks\""));
        assertNotNull(result.meta().assumptions());
    }
}
