package io.sparkadvisor.advisor;

import io.sparkadvisor.advisor.api.TuningAdvisor;
import io.sparkadvisor.advisor.llm.AdviceResponseParser;
import io.sparkadvisor.advisor.llm.LlmAdvisor;
import io.sparkadvisor.advisor.llm.LlmProvider;
import io.sparkadvisor.advisor.llm.MinimaxLlmProvider;
import io.sparkadvisor.advisor.rule.RuleBasedAdvisor;
import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.finding.Severity;
import io.sparkadvisor.report.model.AnalysisResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AdvisorTest {

    private static AnalysisResult resultWithFindings() {
        var stage = new StageAnalysis(1, 10, 9000, 9000, 500, 14000, 18.0, 0,
                53_000_000L, 0, 0, 0.0, 0, 0, 0);
        var sql = new SqlAnalysis(42L, "stmt_x", "select 1", "", 15000, 9000, 2000,
                0.67, 0.8, java.util.Arrays.asList(stage));
        var app = new AnalysisResult.AppSummary("app", "n", 15000, 1, 1, 1, 8);
        var meta = new AnalysisResult.Meta("0.1.0", "now", false, "hdfs:///x");
        var finding = new Finding("R1_DATA_SKEW", "skew", Severity.CRITICAL, 1,
                "Stage 1 is skewed.", java.util.Collections.singletonMap("durationSkewRatio", "18.0"),
                java.util.Arrays.asList(Recommendation.conf("set spark.sql.adaptive.skewJoin.enabled=true",
                        "split skewed partitions", "big win")));
        return new AnalysisResult(app, sql, java.util.Arrays.asList(finding), null, null, null, meta);
    }

    @Test
    void ruleBasedAdvisorSummarizesAndConsolidates() {
        TuningAdvisor advisor = new RuleBasedAdvisor();
        var advice = advisor.advise(resultWithFindings());
        assertEquals("rule-based", advice.provider());
        assertTrue(advice.summary().contains("critical"));
        assertFalse(advice.recommendations().isEmpty());
    }

    @Test
    void ruleBasedHandlesNoFindings() {
        var stage = new StageAnalysis(1, 8, 1000, 520, 500, 4000, 1.04, 1.1, 0, 0, 0,
                0.02, 0, 0, 0);
        var sql = new SqlAnalysis(1, "s", "select 1", "", 1000, 900, 800, 0.1, 0.85, java.util.Arrays.asList(stage));
        var app = new AnalysisResult.AppSummary("a", "n", 1000, 1, 1, 1, 8);
        var meta = new AnalysisResult.Meta("0.1.0", "now", false, "x");
        var r = new AnalysisResult(app, sql, new java.util.ArrayList<>(), null, null, null, meta);
        var advice = new RuleBasedAdvisor().advise(r);
        assertTrue(advice.summary().toLowerCase().contains("healthy")
                || advice.summary().toLowerCase().contains("no rule findings"));
    }

    @Test
    void llmAdvisorParsesJsonResponse() {
        // Stub provider returns a well-formed JSON answer.
        LlmProvider stub = new LlmProvider() {
            public String name() { return "llm:test"; }
            public String complete(String s, String u) {
                return "```json\n{\"summary\":\"Skewed join key.\",\"recommendations\":["
                        + "{\"type\":\"SQL_REWRITE\",\"action\":\"salt the key\","
                        + "\"rationale\":\"spreads the hot key\",\"expectedImpact\":\"large\"}]}\n```";
            }
        };
        var advice = new LlmAdvisor(stub).advise(resultWithFindings());
        assertEquals("llm:test", advice.provider());
        assertEquals("Skewed join key.", advice.summary());
        assertEquals(1, advice.recommendations().size());
        assertEquals(Recommendation.Type.SQL_REWRITE, advice.recommendations().get(0).type());
    }

    @Test
    void llmAdvisorDegradesGracefullyOnProviderError() {
        LlmProvider failing = new LlmProvider() {
            public String name() { return "llm:test"; }
            public String complete(String s, String u) throws Exception {
                throw new RuntimeException("network down");
            }
        };
        var advice = new LlmAdvisor(failing).advise(resultWithFindings());
        assertNotNull(advice);                       // never throws
        assertEquals("llm:test", advice.provider());
        assertTrue(advice.summary().toLowerCase().contains("unavailable"));
    }

    @Test
    void llmAdvisorHandlesNullProvider() {
        var advice = new LlmAdvisor(null).advise(resultWithFindings());
        assertNotNull(advice);
        assertEquals("llm:none", advice.provider());
    }

    @Test
    void extractJsonStripsFencesAndProse() {
        assertEquals("{\"a\":1}", AdviceResponseParser.extractJson("```json\n{\"a\":1}\n```"));
        assertEquals("{\"a\":1}", AdviceResponseParser.extractJson("Sure:\n{\"a\":1}\nDone"));
        assertEquals("{}", AdviceResponseParser.extractJson("no json"));
        assertEquals("{}", AdviceResponseParser.extractJson(null));
    }

    @Test
    void factorySelectsByMode() {
        assertEquals(null, AdvisorFactory.forMode("none"));
        assertEquals("rule-based", AdvisorFactory.forMode("rule").name());
        assertEquals("rule-based", AdvisorFactory.forMode(null).name());
        assertEquals("llm:minimax-m2.5", AdvisorFactory.forMode("llm").name());
        assertEquals("llm:claude", AdvisorFactory.forMode("llm:claude").name());
    }

    @Test
    void minimaxProviderIsDefaultAndRequiresApiKey() {
        var provider = new MinimaxLlmProvider(null, null, null);
        assertEquals("llm:minimax-m2.5", provider.name());
        assertThrows(IllegalStateException.class, () -> provider.complete("s", "u"));
    }
}
