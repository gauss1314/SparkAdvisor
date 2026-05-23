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
        Files.writeString(out, render(result));
    }

    public String render(AnalysisResult r) throws IOException {
        StringBuilder h = new StringBuilder(8192);
        h.append("<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">");
        h.append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
        h.append("<title>SparkAdvisor Report</title>");
        h.append("<style>").append(css()).append("</style></head><body>");
        h.append(renderBody(r));
        h.append("</body></html>");
        return h.toString();
    }

    /**
     * Render just the report content (header + sections), without the html/head/body wrapper.
     * Used by the History Server tab, which supplies its own page chrome and only needs the
     * inner HTML. The standalone {@link #render} wraps this in a full document.
     */
    public String renderBody(AnalysisResult r) throws IOException {
        StringBuilder h = new StringBuilder(8192);
        h.append("<header><h1>SparkAdvisor</h1>");
        h.append("<div class=\"sub\">").append(esc(r.meta().generatedAt()))
                .append(" &middot; v").append(esc(r.meta().sparkAdvisorVersion())).append("</div></header>");

        if (r.meta().incomplete()) {
            h.append("<div class=\"banner warn\">Event log appears incomplete or truncated; "
                    + "some metrics may be missing and confidence is reduced.</div>");
        }

        appOverview(h, r);
        if (r.targetSql() != null) {
            sqlOverview(h, r.targetSql());
            criticalPath(h, r.targetSql());
            hardMetrics(h, r.targetSql());
            stageTable(h, r.targetSql());
        } else {
            h.append("<section><p class=\"muted\">No target SQL selected.</p></section>");
        }
        findings(h, r.findings());
        predictions(h, r);
        aiPlaceholder(h, r);
        embeddedJson(h, r);
        return h.toString();
    }

    /** The report stylesheet, exposed so embedders (SHS tab) can inline it. */
    public String stylesheet() {
        return css();
    }

    // ---- Sections --------------------------------------------------------------

    private void appOverview(StringBuilder h, AnalysisResult r) {
        var a = r.app();
        h.append("<section><h2>Application</h2><div class=\"grid\">");
        kv(h, "Name", esc(a.appName()));
        kv(h, "App ID", esc(a.appId()));
        kv(h, "Duration", duration(a.durationMs()));
        kv(h, "SQL executions", String.valueOf(a.sqlExecutionCount()));
        kv(h, "Jobs", String.valueOf(a.jobCount()));
        kv(h, "Stages", String.valueOf(a.stageCount()));
        kv(h, "Available cores", String.valueOf(a.availableCores()));
        h.append("</div></section>");
    }

    private void sqlOverview(StringBuilder h, SqlAnalysis s) {
        h.append("<section><h2>Target SQL</h2><div class=\"grid\">");
        kv(h, "Execution ID", String.valueOf(s.executionId()));
        kv(h, "StatementID", s.statementId() == null ? "&mdash;" : esc(s.statementId()));
        kv(h, "Duration", duration(s.wallClockMs()));
        kv(h, "Stages", String.valueOf(s.stages().size()));
        h.append("</div>");
        if (s.description() != null && !s.description().isBlank()) {
            h.append("<details><summary>SQL text</summary><pre class=\"sql\">")
                    .append(esc(s.description())).append("</pre></details>");
        }
        h.append("</section>");
    }

    /**
     * Three-line headroom chart: ideal &le; criticalPath &le; wallClock, rendered as
     * proportional inline-SVG bars. Shows how much time is removable by balancing skew
     * (wall - critical) vs by adding parallelism (critical - ideal).
     */
    private void criticalPath(StringBuilder h, SqlAnalysis s) {
        long max = Math.max(1, s.wallClockMs());
        int w = 600;
        h.append("<section><h2>Critical path &amp; headroom</h2>");
        h.append("<svg viewBox=\"0 0 ").append(w + 140).append(" 130\" class=\"bars\" role=\"img\">");
        bar(h, 0, "Actual (wall clock)", s.wallClockMs(), max, w, "bar-actual");
        bar(h, 1, "Critical path (∞ executors)", s.criticalPathMs(), max, w, "bar-critical");
        bar(h, 2, "Ideal (no skew)", s.idealMs(), max, w, "bar-ideal");
        h.append("</svg>");
        h.append("<p class=\"muted\">Deviation from critical path: <b>")
                .append(pct(s.deviation())).append("</b> &middot; core utilization: <b>")
                .append(pct(s.coreUtilization()))
                .append(s.coreUtilization() < UTIL_LOW ? " (low)" : "")
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

    private void hardMetrics(StringBuilder h, SqlAnalysis s) {
        // Surface the worst stage by each dimension as the headline numbers.
        double maxSkew = s.stages().stream().mapToDouble(StageAnalysis::skewRatio).max().orElse(0);
        double maxGc = s.stages().stream().mapToDouble(StageAnalysis::gcRatio).max().orElse(0);
        long totalSpill = s.stages().stream().mapToLong(StageAnalysis::spillBytes).sum();
        h.append("<section><h2>Hard metrics</h2><div class=\"cards\">");
        card(h, "Max skew ratio", ratio(maxSkew), maxSkew >= SKEW_WARN);
        card(h, "Max GC ratio", pct(maxGc), maxGc >= GC_WARN);
        card(h, "Total spill", bytes(totalSpill), totalSpill > 0);
        card(h, "Core utilization", pct(s.coreUtilization()), s.coreUtilization() < UTIL_LOW);
        h.append("</div></section>");
    }

    private void stageTable(StringBuilder h, SqlAnalysis s) {
        h.append("<section><h2>Stages</h2><table><thead><tr>"
                + "<th>Stage</th><th>Tasks</th><th>Wall</th><th>Max task</th><th>Median</th>"
                + "<th>Skew</th><th>Shuffle R/W</th><th>Spill</th><th>GC</th><th>Sched delay</th>"
                + "</tr></thead><tbody>");
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

    private void findings(StringBuilder h, List<Finding> findings) {
        h.append("<section><h2>Findings</h2>");
        if (findings == null || findings.isEmpty()) {
            h.append("<p class=\"muted\">No rule findings in this build "
                    + "(the rule engine arrives in M2).</p></section>");
            return;
        }
        for (Finding f : findings) {
            String sev = f.severity().name().toLowerCase();
            h.append("<div class=\"finding ").append(sev).append("\">");
            h.append("<div class=\"finding-head\"><span class=\"sev ").append(sev).append("\">")
                    .append(f.severity()).append("</span> ").append(esc(f.explanation()));
            if (f.targetStageId() != null) {
                h.append(" <span class=\"muted\">(stage ").append(f.targetStageId()).append(")</span>");
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

    private void predictions(StringBuilder h, AnalysisResult r) {
        ShufflePartitionPrediction sp = r.shufflePrediction();
        ExecutorScalingPrediction ep = r.executorPrediction();
        if (sp == null && ep == null) {
            return;
        }
        h.append("<section><h2>Predictions (cost-model estimates)</h2>");
        h.append("<p class=\"muted\">These are model-based estimates, not guarantees — "
                + "each carries a confidence level and assumptions.</p>");

        if (sp != null) {
            String verdict = switch (sp.direction()) {
                case FASTER_IF_INCREASED -> "Increasing partitions is predicted to help";
                case FASTER_IF_DECREASED -> "Decreasing partitions is predicted to help";
                case ALREADY_OPTIMAL -> "Current partition count is near-optimal";
                case SKEW_LIMITED -> "Skew-limited: changing partition count is unlikely to help";
            };
            h.append("<div class=\"pred\"><h3>Shuffle partitions — stage ").append(sp.stageId())
                    .append("</h3>");
            h.append("<p><b>").append(esc(verdict)).append("</b> ");
            if (sp.direction() != ShufflePartitionPrediction.Direction.SKEW_LIMITED) {
                h.append("&middot; current ").append(sp.currentPartitions()).append(" (~")
                        .append(duration(sp.estCurrentMs())).append(") → recommended ")
                        .append(sp.recommendedPartitions()).append(" (~")
                        .append(duration(sp.estRecommendedMs())).append("), est. speedup ")
                        .append(pct(sp.estimatedSpeedup()));
            }
            h.append("</p>");
            h.append("<p class=\"muted\">Knob: <code>").append(esc(sp.tunedKnob()))
                    .append("</code> &middot; confidence: <b>").append(sp.confidence())
                    .append("</b></p>");
            assumptions(h, sp.assumptions(), sp.reversalNote());
            h.append("</div>");
        }

        if (ep != null) {
            h.append("<div class=\"pred\"><h3>Executor scaling</h3>");
            h.append("<p>Current cores: ").append(ep.currentCores()).append(" (~")
                    .append(duration(ep.estCurrentMs())).append(") &middot; "
                            + "diminishing returns beyond <b>").append(ep.kneeCores())
                    .append(" cores</b> &middot; confidence: <b>").append(ep.confidence())
                    .append("</b></p>");
            // Simple text curve.
            h.append("<table><thead><tr><th>Cores</th><th>Est. wall clock</th></tr></thead><tbody>");
            for (ExecutorScalingPrediction.Point p : ep.curve()) {
                h.append("<tr>");
                td(h, String.valueOf(p.cores()));
                td(h, duration(p.estMs()));
                h.append("</tr>");
            }
            h.append("</tbody></table>");
            assumptions(h, ep.assumptions(), null);
            h.append("</div>");
        }
        h.append("</section>");
    }

    private void assumptions(StringBuilder h, java.util.List<String> assumptions, String reversalNote) {
        if ((assumptions == null || assumptions.isEmpty()) && reversalNote == null) {
            return;
        }
        h.append("<details><summary>Assumptions</summary><ul class=\"assume\">");
        if (assumptions != null) {
            for (String a : assumptions) {
                h.append("<li>").append(esc(a)).append("</li>");
            }
        }
        if (reversalNote != null) {
            h.append("<li><i>Reverses if:</i> ").append(esc(reversalNote)).append("</li>");
        }
        h.append("</ul></details>");
    }

    private void aiPlaceholder(StringBuilder h, AnalysisResult r) {
        h.append("<section><h2>Tuning advice</h2>");
        var advice = r.aiAdvice();
        if (advice == null) {
            h.append("<p class=\"muted\">Not generated. Run with an advisor "
                    + "(rule-based or LLM) to populate this section; it consumes the JSON below.</p>");
            h.append("</section>");
            return;
        }
        h.append("<p class=\"muted\">Source: <b>").append(esc(advice.provider())).append("</b></p>");
        if (advice.summary() != null && !advice.summary().isBlank()) {
            h.append("<p>").append(esc(advice.summary())).append("</p>");
        }
        if (advice.recommendations() != null && !advice.recommendations().isEmpty()) {
            for (Recommendation rec : advice.recommendations()) {
                h.append("<div class=\"rec\"><b>").append(rec.type()).append(":</b> ")
                        .append(esc(rec.action()));
                if (rec.rationale() != null && !rec.rationale().isBlank()) {
                    h.append(" — <span class=\"muted\">").append(esc(rec.rationale())).append("</span>");
                }
                if (rec.expectedImpact() != null && !rec.expectedImpact().isBlank()) {
                    h.append(" <span class=\"muted\">[").append(esc(rec.expectedImpact())).append("]</span>");
                }
                h.append("</div>");
            }
        }
        h.append("</section>");
    }

    private void embeddedJson(StringBuilder h, AnalysisResult r) throws IOException {
        h.append("<section><h2>Raw analysis (JSON contract)</h2>");
        h.append("<details><summary>Show JSON</summary><pre class=\"json\">")
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
