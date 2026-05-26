package io.sparkadvisor.report.html;

import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.predict.ExecutorScalingPrediction;
import io.sparkadvisor.core.predict.ShufflePartitionPrediction;
import io.sparkadvisor.report.json.JsonReportWriter;
import io.sparkadvisor.report.model.AnalysisResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.sparkadvisor.report.html.Html.bytes;
import static io.sparkadvisor.report.html.Html.duration;
import static io.sparkadvisor.report.html.Html.esc;
import static io.sparkadvisor.report.html.Html.pct;
import static io.sparkadvisor.report.html.Html.ratio;

/**
 * Renders an {@link AnalysisResult} into a single self-contained HTML file.
 *
 * <p>No front-end build chain: inline CSS, inline SVG for the critical-path bars, and the
 * raw {@link AnalysisResult} JSON embedded at the bottom (so the page is also a contract
 * carrier the UI/LLM step can read back). Thresholds drive simple color coding.
 */
public final class HtmlReportWriter {

    // Threshold constants (kept here for M1; analyzer will own canonical thresholds in M2).
    private static final double SKEW_WARN = 5.0;
    private static final double GC_WARN = 0.10;
    private static final double UTIL_LOW = 0.40;

    private final JsonReportWriter jsonWriter = new JsonReportWriter();

    public void write(AnalysisResult result, Path out) throws IOException {
        Files.writeString(out, render(result, isChineseOutput(out)));
    }

    public String render(AnalysisResult r) throws IOException {
        return render(r, false);
    }

    public String render(AnalysisResult r, boolean zh) throws IOException {
        StringBuilder h = new StringBuilder(8192);
        h.append("<!DOCTYPE html>\n<html lang=\"").append(zh ? "zh-CN" : "en")
                .append("\"><head><meta charset=\"utf-8\">");
        h.append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
        h.append("<title>").append(t(zh, "SparkAdvisor Report", "SparkAdvisor 报告"))
                .append("</title>");
        h.append("<style>").append(css()).append("</style></head><body>");
        h.append(renderBody(r, zh));
        h.append("</body></html>");
        return h.toString();
    }

    /**
     * Render just the report content (header + sections), without the html/head/body wrapper.
     * Used by the History Server tab, which supplies its own page chrome and only needs the
     * inner HTML. The standalone {@link #render} wraps this in a full document.
     */
    public String renderBody(AnalysisResult r) throws IOException {
        return renderBody(r, false);
    }

    public String renderBody(AnalysisResult r, boolean zh) throws IOException {
        StringBuilder h = new StringBuilder(8192);
        h.append("<header><h1>SparkAdvisor</h1>");
        h.append("<div class=\"sub\">").append(esc(r.meta().generatedAt()))
                .append(" &middot; v").append(esc(r.meta().sparkAdvisorVersion())).append("</div></header>");

        if (r.meta().incomplete()) {
            h.append("<div class=\"banner warn\">")
                    .append(t(zh,
                            "Event log appears incomplete or truncated; some metrics may be missing and confidence is reduced.",
                            "Event log 可能不完整或被截断；部分指标可能缺失，结论置信度会降低。"))
                    .append("</div>");
        }

        appOverview(h, r, zh);
        if (r.targetSql() != null) {
            sqlOverview(h, r.targetSql(), zh);
            criticalPath(h, r.targetSql(), zh);
            hardMetrics(h, r.targetSql(), zh);
            stageTable(h, r.targetSql(), zh);
        } else {
            h.append("<section><p class=\"muted\">")
                    .append(t(zh, "No target SQL selected.", "未选择目标 SQL。"))
                    .append("</p></section>");
        }
        findings(h, r.findings(), zh);
        predictions(h, r, zh);
        aiPlaceholder(h, r, zh);
        embeddedJson(h, r, zh);
        return h.toString();
    }

    /** The report stylesheet, exposed so embedders (SHS tab) can inline it. */
    public String stylesheet() {
        return css();
    }

    // ---- Sections --------------------------------------------------------------

    private void appOverview(StringBuilder h, AnalysisResult r, boolean zh) {
        var a = r.app();
        h.append("<section><h2>").append(t(zh, "Application", "应用概览"))
                .append("</h2><div class=\"grid\">");
        kv(h, t(zh, "Name", "名称"), esc(a.appName()));
        kv(h, "App ID", esc(a.appId()));
        kv(h, t(zh, "Duration", "持续时间"), duration(a.durationMs()));
        kv(h, t(zh, "SQL executions", "SQL 执行数"), String.valueOf(a.sqlExecutionCount()));
        kv(h, t(zh, "Jobs", "Job 数"), String.valueOf(a.jobCount()));
        kv(h, t(zh, "Stages", "Stage 数"), String.valueOf(a.stageCount()));
        kv(h, t(zh, "Available cores", "可用 Core"), String.valueOf(a.availableCores()));
        h.append("</div></section>");
    }

    private void sqlOverview(StringBuilder h, SqlAnalysis s, boolean zh) {
        h.append("<section><h2>").append(t(zh, "Target SQL", "目标 SQL"))
                .append("</h2><div class=\"grid\">");
        kv(h, "Execution ID", String.valueOf(s.executionId()));
        kv(h, "StatementID", s.statementId() == null ? "&mdash;" : esc(s.statementId()));
        kv(h, t(zh, "Duration", "持续时间"), duration(s.wallClockMs()));
        kv(h, t(zh, "Stages", "Stage 数"), String.valueOf(s.stages().size()));
        h.append("</div>");
        if (s.description() != null && !s.description().trim().isEmpty()) {
            h.append("<details><summary>").append(t(zh, "SQL text", "SQL 文本"))
                    .append("</summary><pre class=\"sql\">")
                    .append(esc(s.description())).append("</pre></details>");
        }
        h.append("</section>");
    }

    /**
     * Three-line headroom chart: ideal &le; criticalPath &le; wallClock, rendered as
     * proportional inline-SVG bars. Shows how much time is removable by balancing skew
     * (wall - critical) vs by adding parallelism (critical - ideal).
     */
    private void criticalPath(StringBuilder h, SqlAnalysis s, boolean zh) {
        long max = Math.max(1, s.wallClockMs());
        int w = 600;
        h.append("<section><h2>")
                .append(t(zh, "Critical path &amp; headroom", "关键路径与优化空间"))
                .append("</h2>");
        h.append("<svg viewBox=\"0 0 ").append(w + 140).append(" 130\" class=\"bars\" role=\"img\">");
        bar(h, 0, t(zh, "Actual (wall clock)", "实际耗时（墙钟）"),
                s.wallClockMs(), max, w, "bar-actual");
        bar(h, 1, t(zh, "Critical path (∞ executors)", "关键路径（无限 Executor）"),
                s.criticalPathMs(), max, w, "bar-critical");
        bar(h, 2, t(zh, "Ideal (no skew)", "理想耗时（无倾斜）"),
                s.idealMs(), max, w, "bar-ideal");
        h.append("</svg>");
        h.append("<p class=\"muted\">")
                .append(t(zh, "Deviation from critical path", "相对关键路径偏离"))
                .append(": <b>")
                .append(pct(s.deviation())).append("</b> &middot; ")
                .append(t(zh, "core utilization", "Core 利用率"))
                .append(": <b>")
                .append(pct(s.coreUtilization()))
                .append(s.coreUtilization() < UTIL_LOW ? t(zh, " (low)", "（偏低）") : "")
                .append("</b></p></section>");
    }

    private void bar(StringBuilder h, int row, String label, long val, long max, int w, String cls) {
        int y = 15 + row * 35;
        int len = (int) Math.round((double) val / (double) max * w);
        h.append("<text x=\"0\" y=\"").append(y + 13).append("\" class=\"bar-label\">")
                .append(esc(label)).append("</text>");
        h.append("<rect x=\"190\" y=\"").append(y).append("\" width=\"").append(Math.max(1, len))
                .append("\" height=\"20\" class=\"").append(cls).append("\"></rect>");
        h.append("<text x=\"").append(190 + Math.max(1, len) + 6).append("\" y=\"").append(y + 15)
                .append("\" class=\"bar-val\">").append(duration(val)).append("</text>");
    }

    private void hardMetrics(StringBuilder h, SqlAnalysis s, boolean zh) {
        // Surface the worst stage by each dimension as the headline numbers.
        double maxSkew = s.stages().stream().mapToDouble(StageAnalysis::skewRatio).max().orElse(0);
        double maxGc = s.stages().stream().mapToDouble(StageAnalysis::gcRatio).max().orElse(0);
        long totalSpill = s.stages().stream().mapToLong(StageAnalysis::spillBytes).sum();
        h.append("<section><h2>").append(t(zh, "Hard metrics", "硬指标"))
                .append("</h2><div class=\"cards\">");
        card(h, t(zh, "Max skew ratio", "最大倾斜比"), ratio(maxSkew), maxSkew >= SKEW_WARN);
        card(h, t(zh, "Max GC ratio", "最大 GC 比例"), pct(maxGc), maxGc >= GC_WARN);
        card(h, t(zh, "Total spill", "总 Spill"), bytes(totalSpill), totalSpill > 0);
        card(h, t(zh, "Core utilization", "Core 利用率"),
                pct(s.coreUtilization()), s.coreUtilization() < UTIL_LOW);
        h.append("</div></section>");
    }

    private void stageTable(StringBuilder h, SqlAnalysis s, boolean zh) {
        h.append("<section><h2>").append(t(zh, "Stages", "Stage 明细"))
                .append("</h2><table><thead><tr>");
        th(h, "Stage");
        th(h, t(zh, "Tasks", "Task 数"));
        th(h, t(zh, "Wall", "墙钟"));
        th(h, t(zh, "Max task", "最大 Task"));
        th(h, t(zh, "Median", "中位数"));
        th(h, t(zh, "Skew", "倾斜"));
        th(h, "Shuffle R/W");
        th(h, "Spill");
        th(h, "GC");
        th(h, t(zh, "Sched delay", "调度延迟"));
        h.append("</tr></thead><tbody>");
        for (StageAnalysis st : s.stagesByDurationDesc()) {
            boolean skewy = st.skewRatio() >= SKEW_WARN;
            h.append("<tr").append(skewy ? " class=\"row-warn\"" : "").append(">");
            td(h, String.valueOf(st.stageId()));
            td(h, String.valueOf(st.numTasks()));
            td(h, duration(st.wallClockMs()));
            td(h, duration(st.maxTaskMs()));
            td(h, duration(st.medianTaskMs()));
            td(h, ratio(st.skewRatio()) + (skewy ? " ⚠" : ""));
            td(h, bytes(st.shuffleReadBytes()) + " / " + bytes(st.shuffleWriteBytes()));
            td(h, bytes(st.spillBytes()));
            td(h, pct(st.gcRatio()));
            td(h, duration(st.schedulingDelayMs()));
            h.append("</tr>");
        }
        h.append("</tbody></table></section>");
    }

    private void findings(StringBuilder h, List<Finding> findings, boolean zh) {
        h.append("<section><h2>").append(t(zh, "Findings", "规则发现")).append("</h2>");
        if (findings == null || findings.isEmpty()) {
            h.append("<p class=\"muted\">")
                    .append(t(zh, "No rule findings.", "没有命中规则发现。"))
                    .append("</p></section>");
            return;
        }
        for (Finding f : findings) {
            String sev = f.severity().name().toLowerCase();
            h.append("<div class=\"finding ").append(sev).append("\">");
            h.append("<div class=\"finding-head\"><span class=\"sev ").append(sev).append("\">")
                    .append(f.severity()).append("</span> ").append(esc(f.explanation()));
            if (f.targetStageId() != null) {
                h.append(" <span class=\"muted\">(").append(t(zh, "stage ", "Stage "))
                        .append(f.targetStageId()).append(")</span>");
            }
            h.append("</div>");
            if (f.recommendations() != null) {
                for (Recommendation rec : f.recommendations()) {
                    h.append("<div class=\"rec\"><b>").append(rec.type()).append(":</b> ")
                            .append(esc(rec.action())).append(" — <span class=\"muted\">")
                            .append(esc(rec.rationale())).append("</span></div>");
                }
            }
            h.append("</div>");
        }
        h.append("</section>");
    }

    private void predictions(StringBuilder h, AnalysisResult r, boolean zh) {
        ShufflePartitionPrediction sp = r.shufflePrediction();
        ExecutorScalingPrediction ep = r.executorPrediction();
        if (sp == null && ep == null) {
            return;
        }
        h.append("<section><h2>")
                .append(t(zh, "Predictions (cost-model estimates)", "预测（成本模型估计）"))
                .append("</h2>");
        h.append("<p class=\"muted\">")
                .append(t(zh,
                        "These are model-based estimates, not guarantees; each carries a confidence level and assumptions.",
                        "这些是基于成本模型的估计，不是保证；每项预测都带有置信度和假设。"))
                .append("</p>");

        if (sp != null) {
            String verdict;
            if (sp.direction() == ShufflePartitionPrediction.Direction.FASTER_IF_INCREASED) {
                verdict = t(zh, "Increasing partitions is predicted to help", "预计增加分区数会有帮助");
            } else if (sp.direction() == ShufflePartitionPrediction.Direction.FASTER_IF_DECREASED) {
                verdict = t(zh, "Decreasing partitions is predicted to help", "预计减少分区数会有帮助");
            } else if (sp.direction() == ShufflePartitionPrediction.Direction.ALREADY_OPTIMAL) {
                verdict = t(zh, "Current partition count is near-optimal", "当前分区数接近最优");
            } else {
                verdict = t(zh, "Skew-limited: changing partition count is unlikely to help", "受倾斜限制：调整分区数大概率无效");
            }
            h.append("<div class=\"pred\"><h3>")
                    .append(t(zh, "Shuffle partitions", "Shuffle 分区"))
                    .append(" — stage ").append(sp.stageId())
                    .append("</h3>");
            h.append("<p><b>").append(esc(verdict)).append("</b> ");
            if (sp.direction() != ShufflePartitionPrediction.Direction.SKEW_LIMITED) {
                h.append("&middot; ").append(t(zh, "current", "当前")).append(" ")
                        .append(sp.currentPartitions()).append(" (~")
                        .append(duration(sp.estCurrentMs())).append(") → ")
                        .append(t(zh, "recommended", "建议")).append(" ")
                        .append(sp.recommendedPartitions()).append(" (~")
                        .append(duration(sp.estRecommendedMs())).append("), ")
                        .append(t(zh, "est. speedup", "预计加速"))
                        .append(" ")
                        .append(pct(sp.estimatedSpeedup()));
            }
            h.append("</p>");
            h.append("<p class=\"muted\">").append(t(zh, "Knob", "调参项"))
                    .append(": <code>").append(esc(sp.tunedKnob()))
                    .append("</code> &middot; ").append(t(zh, "confidence", "置信度"))
                    .append(": <b>").append(sp.confidence())
                    .append("</b></p>");
            assumptions(h, sp.assumptions(), sp.reversalNote(), zh);
            h.append("</div>");
        }

        if (ep != null) {
            h.append("<div class=\"pred\"><h3>")
                    .append(t(zh, "Executor scaling", "Executor 伸缩"))
                    .append("</h3>");
            h.append("<p>").append(t(zh, "Current cores", "当前 Core 数"))
                    .append(": ").append(ep.currentCores()).append(" (~")
                    .append(duration(ep.estCurrentMs())).append(") &middot; ")
                    .append(t(zh, "diminishing returns beyond", "超过后收益递减"))
                    .append(" <b>").append(ep.kneeCores())
                    .append(" cores</b> &middot; ").append(t(zh, "confidence", "置信度"))
                    .append(": <b>").append(ep.confidence())
                    .append("</b></p>");
            // Simple text curve.
            h.append("<table><thead><tr>");
            th(h, t(zh, "Cores", "Core 数"));
            th(h, t(zh, "Est. wall clock", "估计墙钟"));
            h.append("</tr></thead><tbody>");
            for (ExecutorScalingPrediction.Point p : ep.curve()) {
                h.append("<tr>");
                td(h, String.valueOf(p.cores()));
                td(h, duration(p.estMs()));
                h.append("</tr>");
            }
            h.append("</tbody></table>");
            assumptions(h, ep.assumptions(), null, zh);
            h.append("</div>");
        }
        h.append("</section>");
    }

    private void assumptions(StringBuilder h, java.util.List<String> assumptions,
                             String reversalNote, boolean zh) {
        if ((assumptions == null || assumptions.isEmpty()) && reversalNote == null) {
            return;
        }
        h.append("<details><summary>").append(t(zh, "Assumptions", "假设"))
                .append("</summary><ul class=\"assume\">");
        if (assumptions != null) {
            for (String a : assumptions) {
                h.append("<li>").append(esc(a)).append("</li>");
            }
        }
        if (reversalNote != null) {
            h.append("<li><i>").append(t(zh, "Reverses if:", "反转条件："))
                    .append("</i> ").append(esc(reversalNote)).append("</li>");
        }
        h.append("</ul></details>");
    }

    private void aiPlaceholder(StringBuilder h, AnalysisResult r, boolean zh) {
        h.append("<section><h2>").append(t(zh, "Tuning advice", "调优建议")).append("</h2>");
        var advice = r.aiAdvice();
        if (advice == null) {
            h.append("<p class=\"muted\">")
                    .append(t(zh,
                            "Not generated. Run with an advisor (rule-based or LLM) to populate this section; it consumes the JSON below.",
                            "未生成。使用 advisor（规则或 LLM）运行后会填充本节；advisor 只消费下方 JSON 契约。"))
                    .append("</p>");
            h.append("</section>");
            return;
        }
        h.append("<p class=\"muted\">").append(t(zh, "Source", "来源"))
                .append(": <b>").append(esc(advice.provider())).append("</b></p>");
        if (advice.summary() != null && !advice.summary().trim().isEmpty()) {
            h.append("<p>").append(esc(advice.summary())).append("</p>");
        }
        if (advice.recommendations() != null && !advice.recommendations().isEmpty()) {
            for (Recommendation rec : advice.recommendations()) {
                h.append("<div class=\"rec\"><b>").append(rec.type()).append(":</b> ")
                        .append(esc(rec.action()));
                if (rec.rationale() != null && !rec.rationale().trim().isEmpty()) {
                    h.append(" — <span class=\"muted\">").append(esc(rec.rationale())).append("</span>");
                }
                if (rec.expectedImpact() != null && !rec.expectedImpact().trim().isEmpty()) {
                    h.append(" <span class=\"muted\">[").append(esc(rec.expectedImpact())).append("]</span>");
                }
                h.append("</div>");
            }
        }
        h.append("</section>");
    }

    private void embeddedJson(StringBuilder h, AnalysisResult r, boolean zh) throws IOException {
        h.append("<section><h2>")
                .append(t(zh, "Raw analysis (JSON contract)", "原始分析（JSON 契约）"))
                .append("</h2>");
        h.append("<details><summary>").append(t(zh, "Show JSON", "显示 JSON"))
                .append("</summary><pre class=\"json\">")
                .append(esc(jsonWriter.toJson(r))).append("</pre></details></section>");
    }

    // ---- small builders --------------------------------------------------------

    private void kv(StringBuilder h, String k, String v) {
        h.append("<div class=\"kv\"><span class=\"k\">").append(k)
                .append("</span><span class=\"v\">").append(v).append("</span></div>");
    }

    private void card(StringBuilder h, String label, String value, boolean warn) {
        h.append("<div class=\"card").append(warn ? " warn" : "").append("\">")
                .append("<div class=\"card-v\">").append(value).append("</div>")
                .append("<div class=\"card-l\">").append(label).append("</div></div>");
    }

    private void td(StringBuilder h, String v) {
        h.append("<td>").append(v).append("</td>");
    }

    private void th(StringBuilder h, String v) {
        h.append("<th>").append(v).append("</th>");
    }

    private static String t(boolean zh, String en, String zhText) {
        return zh ? zhText : en;
    }

    private static boolean isChineseOutput(Path out) {
        if (out == null || out.getFileName() == null) {
            return false;
        }
        return out.getFileName().toString().contains("_zh");
    }

    private String css() {
        return """
            :root{--bg:#0f1419;--panel:#161b22;--line:#2b333d;--fg:#e6edf3;--muted:#8b949e;
              --accent:#4a9eff;--warn:#f0a020;--crit:#f04848;--ideal:#3fb950;--critical:#4a9eff;}
            *{box-sizing:border-box}
            body{margin:0;background:var(--bg);color:var(--fg);
              font:14px/1.5 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif}
            header{padding:20px 24px;border-bottom:1px solid var(--line)}
            h1{margin:0;font-size:20px;letter-spacing:.5px}
            h2{font-size:15px;margin:0 0 12px;color:var(--accent)}
            .sub{color:var(--muted);font-size:12px;margin-top:4px}
            section{padding:18px 24px;border-bottom:1px solid var(--line)}
            .banner{padding:10px 24px;font-size:13px}
            .banner.warn{background:rgba(240,160,32,.12);color:var(--warn);
              border-bottom:1px solid var(--line)}
            .grid{display:flex;flex-wrap:wrap;gap:10px 28px}
            .kv{display:flex;flex-direction:column}.kv .k{color:var(--muted);font-size:11px}
            .kv .v{font-size:15px;font-weight:600}
            .cards{display:flex;flex-wrap:wrap;gap:12px}
            .card{background:var(--panel);border:1px solid var(--line);border-radius:8px;
              padding:12px 16px;min-width:140px}
            .card.warn{border-color:var(--warn)}
            .card-v{font-size:20px;font-weight:700}.card-l{color:var(--muted);font-size:12px}
            table{width:100%;border-collapse:collapse;font-size:13px}
            th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--line)}
            th{color:var(--muted);font-weight:600}
            tr.row-warn td{background:rgba(240,72,72,.08)}
            .muted{color:var(--muted)}
            pre{background:var(--panel);border:1px solid var(--line);border-radius:6px;
              padding:12px;overflow:auto;font-size:12px;max-height:360px}
            pre.sql{white-space:pre-wrap}
            details summary{cursor:pointer;color:var(--accent);margin:6px 0}
            .bars{max-width:100%;font-size:12px}
            .bar-label{fill:var(--muted)}.bar-val{fill:var(--fg)}
            .bar-actual{fill:var(--crit)}.bar-critical{fill:var(--critical)}.bar-ideal{fill:var(--ideal)}
            .finding{background:var(--panel);border:1px solid var(--line);border-left-width:3px;
              border-radius:6px;padding:10px 14px;margin-bottom:10px}
            .finding.critical{border-left-color:var(--crit)}
            .finding.warn{border-left-color:var(--warn)}
            .finding.info{border-left-color:var(--accent)}
            .sev{font-size:11px;font-weight:700;padding:1px 6px;border-radius:4px}
            .sev.critical{background:var(--crit);color:#fff}
            .sev.warn{background:var(--warn);color:#000}
            .sev.info{background:var(--accent);color:#fff}
            .rec{margin-top:6px;font-size:13px}
            .pred{background:var(--panel);border:1px solid var(--line);border-radius:6px;
              padding:10px 14px;margin-bottom:12px}
            .pred h3{font-size:13px;margin:0 0 6px;color:var(--fg)}
            code{background:rgba(255,255,255,.06);padding:1px 5px;border-radius:4px;font-size:12px}
            ul.assume{margin:6px 0;padding-left:18px;color:var(--muted);font-size:12px}
            """;
    }
}
