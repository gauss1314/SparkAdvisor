package io.sparkadvisor.advisor.rule;

import io.sparkadvisor.advisor.api.TuningAdvisor;
import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.finding.Severity;
import io.sparkadvisor.core.predict.ShufflePartitionPrediction;
import io.sparkadvisor.report.i18n.ReportLanguage;
import io.sparkadvisor.report.i18n.ReportText;
import io.sparkadvisor.report.model.AnalysisResult;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Deterministic, offline advisor. Synthesizes a plain-language summary and a de-duplicated,
 * priority-ordered recommendation list from the analysis's findings and predictions.
 *
 * <p>Always available (no network, no API key), so it is the default advisor. The LLM advisor
 * can replace or augment it, returning the same {@link AnalysisResult.AiAdvice} shape.
 */
public final class RuleBasedAdvisor implements TuningAdvisor {

    private final ReportLanguage language;

    public RuleBasedAdvisor() {
        this(ReportLanguage.EN);
    }

    public RuleBasedAdvisor(ReportLanguage language) {
        this.language = language == null ? ReportLanguage.EN : language;
    }

    @Override
    public String name() {
        return "rule-based";
    }

    @Override
    public AnalysisResult.AiAdvice advise(AnalysisResult r) {
        List<Finding> findings = r.findings() == null ? new java.util.ArrayList<>() : r.findings();
        String summary = summarize(r, findings);

        // Consolidate recommendations: criticals first, then warns, de-duplicated by action.
        List<Recommendation> recs = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (Severity sev : new Severity[]{Severity.CRITICAL, Severity.WARN, Severity.INFO}) {
            for (Finding f : findings) {
                if (f.severity() != sev) continue;
                for (Recommendation rec : f.recommendations()) {
                    if (seen.add(rec.action())) {
                        recs.add(ReportText.localize(rec, language));
                    }
                }
            }
        }
        return new AnalysisResult.AiAdvice(name(), summary, recs);
    }

    private String summarize(AnalysisResult r, List<Finding> findings) {
        StringBuilder s = new StringBuilder();
        if (r.targetSql() != null) {
            SqlAnalysis sql = r.targetSql();
            if (language.isChinese()) {
                s.append(String.format(java.util.Locale.ROOT,
                        "该 SQL 耗时 %d ms，关键路径为 %d ms；按当前模型估计，最多约 %.0f%% 的墙钟时间存在优化空间。 ",
                        sql.wallClockMs(), sql.criticalPathMs(),
                        Math.max(0, sql.deviation()) * 100));
            } else {
                s.append(String.format(java.util.Locale.ROOT,
                        "This SQL ran in %d ms; its critical path is %d ms, so up to %.0f%% of the "
                                + "wall clock is potentially removable. ",
                        sql.wallClockMs(), sql.criticalPathMs(),
                        Math.max(0, sql.deviation()) * 100));
            }
        }
        long crit = findings.stream().filter(f -> f.severity() == Severity.CRITICAL).count();
        long warn = findings.stream().filter(f -> f.severity() == Severity.WARN).count();
        if (findings.isEmpty()) {
            s.append(language.isChinese()
                    ? "未命中任何规则；按当前阈值看，该查询没有明显异常。"
                    : "No rule findings were triggered; the query looks healthy by the current thresholds.");
            return s.toString();
        }
        if (language.isChinese()) {
            s.append(String.format(java.util.Locale.ROOT,
                    "共发现 %d 个严重问题和 %d 个警告问题。 ", crit, warn));
        } else {
            s.append(String.format(java.util.Locale.ROOT,
                    "%d critical and %d warning issue(s) were found. ", crit, warn));
        }

        // Lead with the most severe issue's explanation.
        findings.stream()
                .filter(f -> f.severity() == Severity.CRITICAL)
                .findFirst()
                .ifPresent(f -> s.append(language.isChinese() ? "首要处理：" : "Top priority: ")
                        .append(ReportText.findingExplanation(f, language)).append(" "));

        // Reference the shuffle prediction direction if present.
        ShufflePartitionPrediction sp = r.shufflePrediction();
        if (sp != null && sp.direction() == ShufflePartitionPrediction.Direction.SKEW_LIMITED) {
            s.append(language.isChinese()
                    ? "注意：这里受倾斜限制，单纯重新分区大概率无效。"
                    : "Note: repartitioning is unlikely to help here because the stage is skew-limited.");
        }
        return s.toString().trim();
    }
}
