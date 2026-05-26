package io.sparkadvisor.report;

import io.sparkadvisor.core.metrics.Distribution;
import io.sparkadvisor.core.metrics.MetricDistributionBuilder;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.Job;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.core.model.Stage;
import io.sparkadvisor.core.model.TaskMetricStats;
import io.sparkadvisor.report.html.HtmlReportWriter;
import io.sparkadvisor.report.json.JsonReportWriter;
import io.sparkadvisor.report.model.AnalysisResult;
import io.sparkadvisor.report.model.AnalysisResultBuilder;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportPipelineTest {

    private static Distribution dist(long... vals) {
        var b = new MetricDistributionBuilder();
        for (long v : vals) b.add(v);
        return b.build();
    }

    private static Stage skewedStage() {
        // 9 fast tasks + 1 straggler -> high skew.
        Distribution dur = dist(500, 500, 500, 500, 500, 500, 500, 500, 500, 9000);
        var stats = new TaskMetricStats(dur, dist(1_000_000L, 1_000_000L, 50_000_000L),
                Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY,
                dist(0, 0, 2_000_000_000L), Distribution.EMPTY, dist(50, 60, 2000), Distribution.EMPTY);
        return new Stage(1, 0, 10, new java.util.ArrayList<>(), 3000, 3500, 16000, 53_000_000L, 0, stats);
    }

    private static ApplicationModel demoApp() {
        SqlExecution sql = new SqlExecution(42L, "20260521_demo",
                "/* 20260521_demo */ select c, count(*) from t group by c", "", 1000L, 16000L,
                false, java.util.Arrays.asList(7L));
        Job job = new Job(7, 42L, java.util.Arrays.asList(1), 1000L, 16000L, false);
        Map<String, String> conf = new LinkedHashMap<>();
        conf.put("spark.executor.instances", "4");
        conf.put("spark.executor.cores", "2");
        return new ApplicationModel("app-demo", "Demo", 0L, 17000L, false, conf,
                java.util.Arrays.asList(sql), java.util.Arrays.asList(job), java.util.Arrays.asList(skewedStage()), java.util.new java.util.ArrayList<>(),
                java.util.new java.util.ArrayList<>());
    }

    @Test
    void buildsResultWithCriticalPathOrdering() {
        AnalysisResult r = new AnalysisResultBuilder(demoApp(), "hdfs:///demo").build(demoApp().sqlExecutions().get(0));
        var s = r.targetSql();
        assertNotNull(s);
        assertEquals(42L, s.executionId());
        assertEquals("20260521_demo", s.statementId());
        // ideal <= critical <= wall
        assertTrue(s.idealMs() <= s.criticalPathMs(), "ideal<=critical");
        assertTrue(s.criticalPathMs() <= s.wallClockMs(), "critical<=wall");
        // critical path must include the 9s straggler.
        assertTrue(s.criticalPathMs() >= 9000, "critical includes straggler");
    }

    @Test
    void rendersSelfContainedHtml() throws Exception {
        AnalysisResult r = new AnalysisResultBuilder(demoApp(), "hdfs:///demo").build(demoApp().sqlExecutions().get(0));
        String html = new HtmlReportWriter().render(r);
        assertTrue(html.startsWith("<!DOCTYPE html>"));
        assertTrue(html.trim().endsWith("</html>"));
        assertTrue(html.contains("20260521_demo"), "shows statementId");
        assertTrue(html.contains("row-warn"), "skewed stage flagged");
        assertTrue(html.contains("Critical path"), "has critical path section");
    }

    @Test
    void rendersChineseHtml() throws Exception {
        AnalysisResult r = new AnalysisResultBuilder(demoApp(), "hdfs:///demo").build(demoApp().sqlExecutions().get(0));
        String html = new HtmlReportWriter().render(r, true);
        assertTrue(html.contains("lang=\"zh-CN\""));
        assertTrue(html.contains("应用概览"));
        assertTrue(html.contains("关键路径与优化空间"));
    }

    @Test
    void jsonContractSerializes() throws Exception {
        AnalysisResult r = new AnalysisResultBuilder(demoApp(), "hdfs:///demo").build(demoApp().sqlExecutions().get(0));
        String json = new JsonReportWriter().toJson(r);
        assertTrue(json.contains("\"executionId\""), "json has executionId");
        assertTrue(json.contains("20260521_demo"), "json has statementId");
        assertTrue(json.contains("\"criticalPathMs\""), "json has critical path");
    }

    @Test
    void handlesNoTargetSql() throws Exception {
        AnalysisResult r = new AnalysisResultBuilder(demoApp(), "hdfs:///demo").build(null);
        assertEquals(null, r.targetSql());
        String html = new HtmlReportWriter().render(r);
        assertTrue(html.contains("No target SQL"), "graceful no-target rendering");
    }
}
