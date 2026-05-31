package io.sparkadvisor.monitor.render;

import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.util.Strings;
import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;
import io.sparkadvisor.report.i18n.ReportLanguage;
import io.sparkadvisor.report.i18n.ReportText;
import io.sparkadvisor.report.model.AnalysisResult;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.ToLongFunction;

/**
 * Self-contained HTML renderer for queue-level reports.
 */
public final class QueueHtmlWriter {

    private final QueueJsonWriter jsonWriter = new QueueJsonWriter();

    public void write(QueueAnalysisResult result, Path out) throws IOException {
        write(result, out, ReportLanguage.fromOutputPath(out));
    }

    public void write(QueueAnalysisResult result, Path out, ReportLanguage language) throws IOException {
        Files.write(out, render(result, language).getBytes(StandardCharsets.UTF_8));
    }

    public String render(QueueAnalysisResult r) throws IOException {
        return render(r, ReportLanguage.EN);
    }

    public String render(QueueAnalysisResult r, boolean zh) throws IOException {
        return render(r, zh ? ReportLanguage.ZH : ReportLanguage.EN);
    }

    public String render(QueueAnalysisResult r, ReportLanguage language) throws IOException {
        boolean zh = language != null && language.isChinese();
        return "<!DOCTYPE html>\n<html lang=\"" + (zh ? "zh-CN" : "en")
                + "\"><head><meta charset=\"utf-8\">"
                + "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
                + "<title>" + t(zh, "SparkAdvisor Queue Report", "SparkAdvisor 队列报告")
                + "</title><style>" + stylesheet() + "</style>"
                + "</head><body>" + renderBody(r, language) + "</body></html>";
    }

    public String renderBody(QueueAnalysisResult r) throws IOException {
        return renderBody(r, ReportLanguage.EN);
    }

    public String renderBody(QueueAnalysisResult r, boolean zh) throws IOException {
        return renderBody(r, zh ? ReportLanguage.ZH : ReportLanguage.EN);
    }

    public String renderBody(QueueAnalysisResult r, ReportLanguage language) throws IOException {
        boolean zh = language != null && language.isChinese();
        StringBuilder h = new StringBuilder(16_384);
        h.append("<header><h1>")
                .append(t(zh, "SparkAdvisor Queue", "SparkAdvisor 队列"))
                .append("</h1><div class=\"sub\">")
                .append(esc(r.meta().generatedAt())).append(" &middot; v")
                .append(esc(r.meta().sparkAdvisorVersion())).append("</div></header>");
        if (r.meta().runningSnapshot()) {
            h.append("<div class=\"banner warn\">")
                    .append(t(zh, "Running snapshot @ ", "运行中快照 @ "))
                    .append(esc(clockTime(r.meta().generatedAt())))
                    .append(t(zh, "; ", "，含 "))
                    .append(r.summary().runningQueries())
                    .append(t(zh,
                            " SQL execution(s) are still open and are excluded from completed-query statistics.",
                            " 条未完成查询；这些查询不会混入已完成查询统计。"))
                    .append("</div>");
        }
        if (r.meta().incomplete()) {
            h.append("<div class=\"banner warn\">")
                    .append(t(zh,
                            "Event log is incomplete or may be truncated; queue-level confidence is reduced.",
                            "Event log 不完整或可能被截断；队列级结论置信度会降低。"))
                    .append("</div>");
        }
        if (!r.meta().incremental() || !Strings.isBlank(r.meta().degradedReason())
                || r.meta().deepCoveragePct() < 0.50) {
            h.append("<div class=\"banner warn\">")
                    .append(t(zh,
                            "Caveat: queue evidence is a snapshot; incremental replay may be unavailable and deep-analysis coverage may be partial.",
                            "注意：队列证据来自快照；增量回放可能不可用，深度分析覆盖率可能不足。"))
                    .append(" ")
                    .append(t(zh, "Deep coverage", "深度覆盖率")).append(": ")
                    .append(pct(r.meta().deepCoveragePct()));
            if (!Strings.isBlank(r.meta().degradedReason())) {
                h.append("<div class=\"muted\">")
                        .append(t(zh, "Reason", "原因")).append(": ")
                        .append(esc(r.meta().degradedReason()))
                        .append("</div>");
            }
            h.append("</div>");
        }
        overview(h, r, zh);
        timeline(h, r, zh);
        bottlenecks(h, r, zh);
        contention(h, r, zh);
        slowQueries(h, r, zh);
        recommendations(h, r, language);
        aiAdvice(h, r, language);
        embeddedJson(h, r, zh);
        return h.toString();
    }

    public String stylesheet() {
        return String.join("\n",
                ":root{--bg:#0f1419;--panel:#161b22;--line:#2b333d;--fg:#e6edf3;--muted:#8b949e;",
                "  --accent:#4a9eff;--warn:#f0a020;--crit:#f04848;--ok:#3fb950}",
                "*{box-sizing:border-box}",
                "body{margin:0;background:var(--bg);color:var(--fg);",
                "  font:14px/1.5 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif}",
                "header{padding:20px 24px;border-bottom:1px solid var(--line)}",
                "h1{margin:0;font-size:20px} h2{font-size:15px;margin:0 0 12px;color:var(--accent)}",
                "h3{font-size:13px;margin:0 0 6px}.sub,.muted{color:var(--muted)}",
                "section{padding:18px 24px;border-bottom:1px solid var(--line)}",
                ".banner{padding:10px 24px;font-size:13px;border-bottom:1px solid var(--line)}",
                ".banner.warn{background:rgba(240,160,32,.12);color:var(--warn)}",
                ".grid{display:flex;flex-wrap:wrap;gap:10px 28px}",
                ".kv{display:flex;flex-direction:column}.k{color:var(--muted);font-size:11px}.v{font-size:15px;font-weight:600}",
                ".cards{display:flex;flex-wrap:wrap;gap:12px}.card{background:var(--panel);border:1px solid var(--line);",
                "  border-radius:8px;padding:12px 16px;min-width:150px}.card.warn{border-color:var(--warn)}",
                ".card-v{font-size:20px;font-weight:700}.card-l{color:var(--muted);font-size:12px}",
                "table{width:100%;border-collapse:collapse;font-size:13px}",
                "th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--line)}",
                "th{color:var(--muted);font-weight:600}",
                ".bar{height:10px;background:rgba(74,158,255,.18);border-radius:4px;overflow:hidden}",
                ".bar>span{display:block;height:100%;background:var(--accent)}",
                ".chart{width:100%;max-width:980px;height:auto;margin:4px 0 14px;display:block}",
                ".axis{stroke:var(--line);stroke-width:1}.chart-label{fill:var(--muted);font-size:11px}",
                ".line-p50{fill:none;stroke:#3fb950;stroke-width:2}.line-p95{fill:none;stroke:#4a9eff;stroke-width:2}",
                ".line-p99{fill:none;stroke:#f0a020;stroke-width:2}.line-util{fill:none;stroke:#f04848;stroke-width:2;stroke-dasharray:5 4}",
                ".legend{display:flex;flex-wrap:wrap;gap:14px;margin:0 0 10px;color:var(--muted);font-size:12px}",
                ".legend b{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px}",
                ".query-link{color:var(--accent);text-decoration:none}.query-link:hover{text-decoration:underline}",
                ".share{min-width:110px}",
                ".rec{background:var(--panel);border:1px solid var(--line);border-left:3px solid var(--accent);",
                "  border-radius:6px;padding:10px 14px;margin-bottom:10px}",
                "pre{background:var(--panel);border:1px solid var(--line);border-radius:6px;padding:12px;overflow:auto;",
                "  font-size:12px;max-height:420px} code{background:rgba(255,255,255,.06);padding:1px 5px;border-radius:4px}",
                "");
    }

    private void overview(StringBuilder h, QueueAnalysisResult r, boolean zh) {
        QueueAnalysisResult.QueueSummary s = r.summary();
        h.append("<section><h2>").append(t(zh, "Queue overview", "队列概览"))
                .append("</h2><div class=\"grid\">");
        kv(h, t(zh, "Name", "名称"), esc(s.appName()));
        kv(h, "App ID", esc(s.appId()));
        kv(h, t(zh, "Window", "时间窗口"), time(s.windowStart()) + " - " + time(s.windowEnd()));
        kv(h, t(zh, "Total SQL", "SQL 总数"), String.valueOf(s.totalQueries()));
        kv(h, t(zh, "Completed", "已完成"), String.valueOf(s.completedQueries()));
        kv(h, t(zh, "Running", "运行中"), String.valueOf(s.runningQueries()));
        kv(h, t(zh, "Fixed cores", "固定 Core"), String.valueOf(s.fixedExecutorCores()));
        h.append("</div><p class=\"muted\">").append(esc(r.meta().assumptions())).append("</p></section>");
        h.append("<section><h2>").append(t(zh, "Queue health", "队列健康度"))
                .append("</h2><div class=\"cards\">");
        card(h, t(zh, "Avg pool utilization", "平均资源池利用率"), pct(r.utilization().avgUtilization()),
                r.utilization().avgUtilization() > 0.85 || r.utilization().avgUtilization() < 0.35);
        card(h, t(zh, "Peak utilization", "峰值利用率"), pct(r.utilization().peakUtilization()),
                r.utilization().peakUtilization() > 0.95);
        card(h, t(zh, "P95 max GC", "P95 最大 GC"), pct(r.resources().p95MaxGcRatio()),
                r.resources().p95MaxGcRatio() > 0.10);
        card(h, t(zh, "CPU efficiency", "CPU 效率"), pct(r.resources().avgCpuEfficiency()),
                r.resources().avgSlotOccupancy() > 0.85 && r.resources().avgCpuEfficiency() < 0.35);
        card(h, t(zh, "Fetch wait", "Fetch 等待"), pct(r.resources().avgFetchWaitRatio()),
                r.resources().avgFetchWaitRatio() > 0.20);
        card(h, t(zh, "Total spill", "总 Spill"), bytes(r.resources().totalSpillBytes()),
                r.resources().totalSpillBytes() > 0);
        card(h, t(zh, "Contention-limited", "争用受限占比"), pct(r.contention().contentionLimitedPct()),
                r.contention().contentionLimitedPct() > 0.25);
        card(h, t(zh, "Deep coverage", "深度覆盖率"), pct(r.meta().deepCoveragePct()),
                r.meta().deepCoveragePct() < 0.50);
        card(h, t(zh, "Global recommendations", "全局建议数"),
                String.valueOf(r.globalRecommendations().size()), false);
        h.append("</div></section>");
    }

    private void timeline(StringBuilder h, QueueAnalysisResult r, boolean zh) {
        h.append("<section><h2>")
                .append(t(zh, "Latency and utilization trend", "延迟与利用率趋势"))
                .append("</h2>");
        timelineChart(h, r.timeline(), zh);
        h.append("<table><thead><tr>");
        th(h, t(zh, "Bucket", "时间桶"));
        th(h, t(zh, "Queries", "查询数"));
        th(h, "P50");
        th(h, "P95");
        th(h, "P99");
        th(h, t(zh, "Slot", "Slot"));
        th(h, "CPU");
        th(h, "Fetch");
        th(h, "GC");
        h.append("</tr></thead><tbody>");
        for (QueueAnalysisResult.HourBucketStat b : r.timeline()) {
            h.append("<tr>");
            td(h, time(b.bucketStart()));
            td(h, String.valueOf(b.queryCount()));
            td(h, duration(b.p50Ms()));
            td(h, duration(b.p95Ms()));
            td(h, duration(b.p99Ms()));
            td(h, pct(b.avgUtilization()));
            td(h, pct(b.cpuEfficiency()));
            td(h, pct(b.fetchWaitRatio()));
            td(h, pct(b.gcRatio()));
            h.append("</tr>");
        }
        h.append("</tbody></table></section>");
    }

    private void bottlenecks(StringBuilder h, QueueAnalysisResult r, boolean zh) {
        h.append("<section><h2>").append(t(zh, "Bottleneck clusters", "瓶颈聚类"))
                .append("</h2>");
        if (r.bottlenecks().isEmpty()) {
            h.append("<p class=\"muted\">")
                    .append(t(zh,
                            "No repeated bottlenecks in the deeply analyzed slow-query set.",
                            "深度分析的慢查询集中没有发现重复瓶颈。"))
                    .append("</p></section>");
            return;
        }
        h.append("<table><thead><tr>");
        th(h, t(zh, "Rule", "规则"));
        th(h, t(zh, "Category", "类别"));
        th(h, t(zh, "Affected", "影响查询数"));
        th(h, t(zh, "Share", "占比"));
        th(h, t(zh, "Scope", "范围"));
        th(h, t(zh, "Coverage", "覆盖率"));
        h.append("</tr></thead><tbody>");
        for (QueueAnalysisResult.BottleneckCluster b : r.bottlenecks()) {
            h.append("<tr>");
            td(h, esc(b.ruleId()));
            td(h, esc(b.category()));
            td(h, String.valueOf(b.affectedQueries()));
            td(h, shareBar(b.affectedPct()));
            td(h, esc(b.scope()));
            td(h, pct(b.sampleCoveragePct()));
            h.append("</tr>");
        }
        if (!r.contention().starvationWindows().isEmpty()) {
            h.append("<h3>").append(t(zh, "Starvation windows", "饥饿窗口"))
                    .append("</h3><table><thead><tr>");
            th(h, t(zh, "Start", "开始"));
            th(h, t(zh, "End", "结束"));
            th(h, t(zh, "Avg util", "平均利用率"));
            h.append("</tr></thead><tbody>");
            for (QueueAnalysisResult.ContentionReport.Window w : r.contention().starvationWindows()) {
                h.append("<tr>");
                td(h, time(w.startTime()));
                td(h, time(w.endTime()));
                td(h, pct(w.avgUtilization()));
                h.append("</tr>");
            }
            h.append("</tbody></table>");
        }
        h.append("<p class=\"muted\">")
                .append(t(zh, "Inefficient-busy queries", "低效占用查询占比"))
                .append(": ").append(pct(r.contention().inefficientBusyPct()))
                .append("</p>");
        h.append("</tbody></table></section>");
    }

    private void contention(StringBuilder h, QueueAnalysisResult r, boolean zh) {
        h.append("<section><h2>").append(t(zh, "Contention", "资源争用")).append("</h2>");
        if (r.contention().hotspots().isEmpty()) {
            h.append("<p class=\"muted\">")
                    .append(t(zh, "No utilization hotspot buckets above 95%.",
                            "没有超过 95% 利用率的热点时间桶。"))
                    .append("</p>");
        } else {
            h.append("<h3>").append(t(zh, "Hotspots", "热点时段"))
                    .append("</h3><table><thead><tr>");
            th(h, t(zh, "Start", "开始"));
            th(h, t(zh, "End", "结束"));
            th(h, t(zh, "Avg util", "平均利用率"));
            h.append("</tr></thead><tbody>");
            for (QueueAnalysisResult.ContentionReport.Window w : r.contention().hotspots()) {
                h.append("<tr>");
                td(h, time(w.startTime()));
                td(h, time(w.endTime()));
                td(h, pct(w.avgUtilization()));
                h.append("</tr>");
            }
            h.append("</tbody></table>");
        }
        h.append("<h3>").append(t(zh, "Top resource hogs", "资源大户"))
                .append("</h3>");
        slowQueryTable(h, r.contention().topResourceHogs(), zh);
        h.append("</section>");
    }

    private void slowQueries(StringBuilder h, QueueAnalysisResult r, boolean zh) {
        h.append("<section><h2>").append(t(zh, "Top slow queries", "最慢查询 Top-N"))
                .append("</h2>");
        slowQueryTable(h, r.topSlowQueries(), zh);
        if (!r.sampledQueries().isEmpty()) {
            h.append("<h3>").append(t(zh, "Deep-analysis sample", "深度分析样本"))
                    .append("</h3>");
            slowQueryTable(h, r.sampledQueries(), zh);
        }
        h.append("</section>");
    }

    private void slowQueryTable(StringBuilder h, List<QueueAnalysisResult.SlowQueryRef> rows,
                                boolean zh) {
        if (rows.isEmpty()) {
            h.append("<p class=\"muted\">")
                    .append(t(zh, "No completed SQL executions.", "没有已完成 SQL。"))
                    .append("</p>");
            return;
        }
        h.append("<table><thead><tr>");
        th(h, "StatementID");
        th(h, "Execution");
        th(h, t(zh, "Duration", "耗时"));
        th(h, t(zh, "Dominant bottleneck", "主要瓶颈"));
        th(h, t(zh, "Contention", "争用"));
        th(h, t(zh, "Deep", "深度"));
        th(h, "Template");
        th(h, "Own core-ms");
        h.append("</tr></thead><tbody>");
        for (QueueAnalysisResult.SlowQueryRef q : rows) {
            h.append("<tr>");
            td(h, statementLink(q.statementId()));
            td(h, String.valueOf(q.executionId()));
            td(h, duration(q.durationMs()));
            td(h, esc(q.dominantBottleneck()));
            td(h, q.contentionLimited() ? t(zh, "yes", "是") : t(zh, "no", "否"));
            td(h, q.deepAnalyzed() ? t(zh, "yes", "是") : t(zh, "no", "否"));
            td(h, esc(q.templateHash()));
            td(h, String.valueOf(q.ownCoreMs()));
            h.append("</tr>");
        }
        h.append("</tbody></table>");
    }

    private void recommendations(StringBuilder h, QueueAnalysisResult r, ReportLanguage language) {
        boolean zh = language != null && language.isChinese();
        h.append("<section><h2>").append(t(zh, "Global recommendations", "全局调参建议"))
                .append("</h2>");
        if (r.globalRecommendations().isEmpty()) {
            h.append("<p class=\"muted\">")
                    .append(t(zh, "No queue-level recommendation met the evidence threshold.",
                            "没有队列级建议达到证据阈值。"))
                    .append("</p></section>");
            return;
        }
        for (QueueAnalysisResult.QueueRecommendation rec : r.globalRecommendations()) {
            Recommendation shown = ReportText.localize(rec.recommendation(), language);
            h.append("<div class=\"rec\"><b>").append(esc(rec.queueRuleId())).append("</b> &middot; ")
                    .append(esc(ReportText.confidence(rec.confidence(), language))).append("<br><code>")
                    .append(esc(shown.action())).append("</code>")
                    .append("<p>").append(esc(shown.rationale())).append("</p>")
                    .append("<p class=\"muted\">").append(t(zh, "Evidence", "证据"))
                    .append(": ").append(esc(ReportText.localizeText(rec.evidence(), language)))
                    .append("<br>").append(t(zh, "Coverage", "预期覆盖"))
                    .append(": ").append(esc(ReportText.localizeText(rec.expectedCoverage(), language)))
                    .append(Strings.isBlank(rec.caveats()) ? "" : "<br>" + t(zh, "Caveats", "限制") + ": "
                            + esc(ReportText.localizeText(rec.caveats(), language)))
                    .append("</p></div>");
        }
        h.append("</section>");
    }

    private void aiAdvice(StringBuilder h, QueueAnalysisResult r, ReportLanguage language) {
        boolean zh = language != null && language.isChinese();
        h.append("<section><h2>").append(t(zh, "AI queue advice", "AI 队列建议")).append("</h2>");
        AnalysisResult.AiAdvice advice = r.aiAdvice();
        if (advice == null) {
            h.append("<p class=\"muted\">")
                    .append(t(zh,
                            "Not generated. Run queue-report with --advise llm to populate this section; it consumes only the JSON below.",
                            "未生成。使用 queue-report --advise llm 后会填充本节；LLM 只消费下方 JSON 契约。"))
                    .append("</p></section>");
            return;
        }
        h.append("<p class=\"muted\">").append(t(zh, "Source", "来源")).append(": <b>")
                .append(esc(advice.provider())).append("</b></p>");
        if (!Strings.isBlank(advice.summary())) {
            h.append("<p>").append(esc(advice.summary())).append("</p>");
        }
        if (advice.recommendations() != null) {
            for (Recommendation rec : advice.recommendations()) {
                Recommendation shown = ReportText.localize(rec, language);
                h.append("<div class=\"rec\"><b>")
                        .append(esc(ReportText.recommendationType(shown.type(), language)))
                        .append(":</b> ")
                        .append(esc(shown.action()));
                if (!Strings.isBlank(shown.rationale())) {
                    h.append("<p>").append(esc(shown.rationale())).append("</p>");
                }
                if (!Strings.isBlank(shown.expectedImpact())) {
                    h.append("<p class=\"muted\">").append(esc(shown.expectedImpact())).append("</p>");
                }
                h.append("</div>");
            }
        }
        h.append("</section>");
    }

    private void embeddedJson(StringBuilder h, QueueAnalysisResult r, boolean zh) throws IOException {
        h.append("<section><h2>")
                .append(t(zh, "Raw queue analysis (JSON contract)", "原始队列分析（JSON 契约）"))
                .append("</h2><details><summary>")
                .append(t(zh, "Show JSON", "显示 JSON"))
                .append("</summary><pre>")
                .append(esc(jsonWriter.toJson(r))).append("</pre></details></section>");
    }

    private void timelineChart(StringBuilder h, List<QueueAnalysisResult.HourBucketStat> buckets,
                               boolean zh) {
        if (buckets == null || buckets.isEmpty()) {
            h.append("<p class=\"muted\">")
                    .append(t(zh, "No timeline buckets.", "没有时间桶数据。"))
                    .append("</p>");
            return;
        }
        long maxLatency = buckets.stream()
                .mapToLong(QueueAnalysisResult.HourBucketStat::p99Ms)
                .max()
                .orElse(1L);
        if (maxLatency <= 0L) {
            maxLatency = 1L;
        }
        h.append("<div class=\"legend\">")
                .append("<span><b style=\"background:#3fb950\"></b>P50</span>")
                .append("<span><b style=\"background:#4a9eff\"></b>P95</span>")
                .append("<span><b style=\"background:#f0a020\"></b>P99</span>")
                .append("<span><b style=\"background:#f04848\"></b>")
                .append(t(zh, "Avg utilization", "平均利用率"))
                .append("</span></div>");
        h.append("<svg class=\"chart\" viewBox=\"0 0 860 250\" role=\"img\" aria-label=\"")
                .append(t(zh, "Latency and utilization chart", "延迟与利用率图表"))
                .append("\">");
        h.append("<line x1=\"50\" y1=\"205\" x2=\"820\" y2=\"205\" class=\"axis\"/>");
        h.append("<line x1=\"50\" y1=\"20\" x2=\"50\" y2=\"205\" class=\"axis\"/>");
        h.append("<line x1=\"820\" y1=\"20\" x2=\"820\" y2=\"205\" class=\"axis\"/>");
        h.append("<text x=\"50\" y=\"232\" class=\"chart-label\">")
                .append(esc(time(buckets.get(0).bucketStart()))).append("</text>");
        h.append("<text x=\"730\" y=\"232\" class=\"chart-label\">")
                .append(esc(time(buckets.get(buckets.size() - 1).bucketStart()))).append("</text>");
        h.append("<text x=\"55\" y=\"17\" class=\"chart-label\">")
                .append(esc(duration(maxLatency))).append("</text>");
        h.append("<text x=\"786\" y=\"17\" class=\"chart-label\">100%</text>");
        polyline(h, "line-p50", buckets, QueueAnalysisResult.HourBucketStat::p50Ms, maxLatency);
        polyline(h, "line-p95", buckets, QueueAnalysisResult.HourBucketStat::p95Ms, maxLatency);
        polyline(h, "line-p99", buckets, QueueAnalysisResult.HourBucketStat::p99Ms, maxLatency);
        utilPolyline(h, buckets);
        h.append("</svg>");
    }

    private void polyline(StringBuilder h, String cls,
                          List<QueueAnalysisResult.HourBucketStat> buckets,
                          ToLongFunction<QueueAnalysisResult.HourBucketStat> valueFn,
                          long maxLatency) {
        h.append("<polyline class=\"").append(cls).append("\" points=\"");
        for (int i = 0; i < buckets.size(); i++) {
            QueueAnalysisResult.HourBucketStat b = buckets.get(i);
            double x = x(i, buckets.size());
            double y = 205.0 - (Math.max(0L, valueFn.applyAsLong(b)) / (double) maxLatency) * 185.0;
            h.append(String.format(java.util.Locale.ROOT, "%.1f,%.1f ", x, y));
        }
        h.append("\"/>");
    }

    private void utilPolyline(StringBuilder h, List<QueueAnalysisResult.HourBucketStat> buckets) {
        h.append("<polyline class=\"line-util\" points=\"");
        for (int i = 0; i < buckets.size(); i++) {
            QueueAnalysisResult.HourBucketStat b = buckets.get(i);
            double x = x(i, buckets.size());
            double util = Math.max(0.0, Math.min(1.0, b.avgUtilization()));
            double y = 205.0 - util * 185.0;
            h.append(String.format(java.util.Locale.ROOT, "%.1f,%.1f ", x, y));
        }
        h.append("\"/>");
    }

    private double x(int index, int size) {
        if (size <= 1) {
            return 50.0;
        }
        return 50.0 + index * (770.0 / (double) (size - 1));
    }

    private String shareBar(double pct) {
        int width = (int) Math.round(Math.max(0.0, Math.min(1.0, pct)) * 100.0);
        return "<div class=\"share\"><div class=\"bar\"><span style=\"width:" + width
                + "%\"></span></div><span class=\"muted\">" + pct(pct) + "</span></div>";
    }

    private String statementLink(String statementId) {
        if (Strings.isBlank(statementId)) {
            return "-";
        }
        String escaped = esc(statementId);
        return "<a class=\"query-link\" href=\"?statementId=" + attr(urlEncode(statementId))
                + "\">" + escaped + "</a>";
    }

    private void kv(StringBuilder h, String k, String v) {
        h.append("<div class=\"kv\"><span class=\"k\">").append(k)
                .append("</span><span class=\"v\">").append(v).append("</span></div>");
    }

    private void card(StringBuilder h, String label, String value, boolean warn) {
        h.append("<div class=\"card").append(warn ? " warn" : "").append("\"><div class=\"card-v\">")
                .append(value).append("</div><div class=\"card-l\">").append(label).append("</div></div>");
    }

    private void td(StringBuilder h, String v) {
        h.append("<td>").append(v).append("</td>");
    }

    private void th(StringBuilder h, String v) {
        h.append("<th>").append(v).append("</th>");
    }

    private static String esc(String s) {
        if (s == null) return "";
        StringBuilder b = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '&': b.append("&amp;"); break;
                case '<': b.append("&lt;"); break;
                case '>': b.append("&gt;"); break;
                case '"': b.append("&quot;"); break;
                case '\'': b.append("&#39;"); break;
                default: b.append(c); break;
            }
        }
        return b.toString();
    }

    private static String attr(String s) {
        return esc(s);
    }

    private static String urlEncode(String s) {
        try {
            return java.net.URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 is not supported", e);
        }
    }

    private static String t(boolean zh, String en, String zhText) {
        return zh ? zhText : en;
    }

    private static String duration(long ms) {
        if (ms < 1000) return ms + "ms";
        long sec = ms / 1000;
        if (sec < 60) return String.format("%.1fs", ms / 1000.0);
        long min = sec / 60;
        long s = sec % 60;
        if (min < 60) return min + "m " + s + "s";
        return (min / 60) + "h " + (min % 60) + "m";
    }

    private static String bytes(long v) {
        if (v < 1024) return v + " B";
        double d = v;
        String[] units = {"KB", "MB", "GB", "TB", "PB"};
        int i = -1;
        do {
            d /= 1024.0;
            i++;
        } while (d >= 1024.0 && i < units.length - 1);
        return String.format("%.2f %s", d, units[i]);
    }

    private static String pct(double ratio) {
        return String.format("%.1f%%", ratio * 100.0);
    }

    private static String time(long epochMs) {
        if (epochMs <= 0L) return "-";
        return java.time.format.DateTimeFormatter.ISO_LOCAL_TIME
                .withZone(java.time.ZoneId.systemDefault())
                .format(java.time.Instant.ofEpochMilli(epochMs));
    }

    private static String clockTime(String instantText) {
        if (Strings.isBlank(instantText)) {
            return "-";
        }
        try {
            return java.time.format.DateTimeFormatter.ISO_LOCAL_TIME
                    .withZone(java.time.ZoneId.systemDefault())
                    .format(java.time.Instant.parse(instantText));
        } catch (RuntimeException e) {
            return instantText;
        }
    }
}
