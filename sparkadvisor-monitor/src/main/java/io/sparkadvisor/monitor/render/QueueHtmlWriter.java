package io.sparkadvisor.monitor.render;

import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Self-contained HTML renderer for queue-level reports.
 */
public final class QueueHtmlWriter {

    private final QueueJsonWriter jsonWriter = new QueueJsonWriter();

    public void write(QueueAnalysisResult result, Path out) throws IOException {
        Files.writeString(out, render(result));
    }

    public String render(QueueAnalysisResult r) throws IOException {
        return "<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">"
                + "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
                + "<title>SparkAdvisor Queue Report</title><style>" + stylesheet() + "</style>"
                + "</head><body>" + renderBody(r) + "</body></html>";
    }

    public String renderBody(QueueAnalysisResult r) throws IOException {
        StringBuilder h = new StringBuilder(16_384);
        h.append("<header><h1>SparkAdvisor Queue</h1><div class=\"sub\">")
                .append(esc(r.meta().generatedAt())).append(" &middot; v")
                .append(esc(r.meta().sparkAdvisorVersion())).append("</div></header>");
        if (r.meta().runningSnapshot()) {
            h.append("<div class=\"banner warn\">Running snapshot; ")
                    .append(r.summary().runningQueries())
                    .append(" SQL execution(s) are still open and are excluded from completed-query statistics.</div>");
        }
        if (r.meta().incomplete()) {
            h.append("<div class=\"banner warn\">Event log is incomplete or may be truncated; "
                    + "queue-level confidence is reduced.</div>");
        }
        overview(h, r);
        timeline(h, r);
        bottlenecks(h, r);
        contention(h, r);
        slowQueries(h, r);
        recommendations(h, r);
        embeddedJson(h, r);
        return h.toString();
    }

    public String stylesheet() {
        return """
            :root{--bg:#0f1419;--panel:#161b22;--line:#2b333d;--fg:#e6edf3;--muted:#8b949e;
              --accent:#4a9eff;--warn:#f0a020;--crit:#f04848;--ok:#3fb950}
            *{box-sizing:border-box}
            body{margin:0;background:var(--bg);color:var(--fg);
              font:14px/1.5 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif}
            header{padding:20px 24px;border-bottom:1px solid var(--line)}
            h1{margin:0;font-size:20px} h2{font-size:15px;margin:0 0 12px;color:var(--accent)}
            h3{font-size:13px;margin:0 0 6px}.sub,.muted{color:var(--muted)}
            section{padding:18px 24px;border-bottom:1px solid var(--line)}
            .banner{padding:10px 24px;font-size:13px;border-bottom:1px solid var(--line)}
            .banner.warn{background:rgba(240,160,32,.12);color:var(--warn)}
            .grid{display:flex;flex-wrap:wrap;gap:10px 28px}
            .kv{display:flex;flex-direction:column}.k{color:var(--muted);font-size:11px}.v{font-size:15px;font-weight:600}
            .cards{display:flex;flex-wrap:wrap;gap:12px}.card{background:var(--panel);border:1px solid var(--line);
              border-radius:8px;padding:12px 16px;min-width:150px}.card.warn{border-color:var(--warn)}
            .card-v{font-size:20px;font-weight:700}.card-l{color:var(--muted);font-size:12px}
            table{width:100%;border-collapse:collapse;font-size:13px}
            th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--line)}
            th{color:var(--muted);font-weight:600}
            .bar{height:10px;background:rgba(74,158,255,.18);border-radius:4px;overflow:hidden}
            .bar>span{display:block;height:100%;background:var(--accent)}
            .rec{background:var(--panel);border:1px solid var(--line);border-left:3px solid var(--accent);
              border-radius:6px;padding:10px 14px;margin-bottom:10px}
            pre{background:var(--panel);border:1px solid var(--line);border-radius:6px;padding:12px;overflow:auto;
              font-size:12px;max-height:420px} code{background:rgba(255,255,255,.06);padding:1px 5px;border-radius:4px}
            """;
    }

    private void overview(StringBuilder h, QueueAnalysisResult r) {
        var s = r.summary();
        h.append("<section><h2>Queue overview</h2><div class=\"grid\">");
        kv(h, "Name", esc(s.appName()));
        kv(h, "App ID", esc(s.appId()));
        kv(h, "Window", time(s.windowStart()) + " - " + time(s.windowEnd()));
        kv(h, "Total SQL", String.valueOf(s.totalQueries()));
        kv(h, "Completed", String.valueOf(s.completedQueries()));
        kv(h, "Running", String.valueOf(s.runningQueries()));
        kv(h, "Fixed cores", String.valueOf(s.fixedExecutorCores()));
        h.append("</div><p class=\"muted\">").append(esc(r.meta().assumptions())).append("</p></section>");
        h.append("<section><h2>Queue health</h2><div class=\"cards\">");
        card(h, "Avg pool utilization", pct(r.utilization().avgUtilization()),
                r.utilization().avgUtilization() > 0.85 || r.utilization().avgUtilization() < 0.35);
        card(h, "Peak utilization", pct(r.utilization().peakUtilization()),
                r.utilization().peakUtilization() > 0.95);
        card(h, "P95 max GC", pct(r.resources().p95MaxGcRatio()),
                r.resources().p95MaxGcRatio() > 0.10);
        card(h, "Total spill", bytes(r.resources().totalSpillBytes()),
                r.resources().totalSpillBytes() > 0);
        card(h, "Contention-limited", pct(r.contention().contentionLimitedPct()),
                r.contention().contentionLimitedPct() > 0.25);
        card(h, "Global recommendations", String.valueOf(r.globalRecommendations().size()), false);
        h.append("</div></section>");
    }

    private void timeline(StringBuilder h, QueueAnalysisResult r) {
        h.append("<section><h2>Hourly latency and utilization</h2><table><thead><tr>"
                + "<th>Bucket</th><th>Queries</th><th>P50</th><th>P95</th><th>P99</th><th>Avg util</th>"
                + "</tr></thead><tbody>");
        for (var b : r.timeline()) {
            h.append("<tr>");
            td(h, time(b.bucketStart()));
            td(h, String.valueOf(b.queryCount()));
            td(h, duration(b.p50Ms()));
            td(h, duration(b.p95Ms()));
            td(h, duration(b.p99Ms()));
            td(h, pct(b.avgUtilization()));
            h.append("</tr>");
        }
        h.append("</tbody></table></section>");
    }

    private void bottlenecks(StringBuilder h, QueueAnalysisResult r) {
        h.append("<section><h2>Bottleneck clusters</h2>");
        if (r.bottlenecks().isEmpty()) {
            h.append("<p class=\"muted\">No repeated bottlenecks in the deeply analyzed slow-query set.</p></section>");
            return;
        }
        h.append("<table><thead><tr><th>Rule</th><th>Category</th><th>Affected</th><th>Share</th></tr></thead><tbody>");
        for (var b : r.bottlenecks()) {
            h.append("<tr>");
            td(h, esc(b.ruleId()));
            td(h, esc(b.category()));
            td(h, String.valueOf(b.affectedQueries()));
            td(h, pct(b.affectedPct()));
            h.append("</tr>");
        }
        h.append("</tbody></table></section>");
    }

    private void contention(StringBuilder h, QueueAnalysisResult r) {
        h.append("<section><h2>Contention</h2>");
        if (r.contention().hotspots().isEmpty()) {
            h.append("<p class=\"muted\">No utilization hotspot buckets above 95%.</p>");
        } else {
            h.append("<h3>Hotspots</h3><table><thead><tr><th>Start</th><th>End</th><th>Avg util</th></tr></thead><tbody>");
            for (var w : r.contention().hotspots()) {
                h.append("<tr>");
                td(h, time(w.startTime()));
                td(h, time(w.endTime()));
                td(h, pct(w.avgUtilization()));
                h.append("</tr>");
            }
            h.append("</tbody></table>");
        }
        h.append("<h3>Top resource hogs</h3>");
        slowQueryTable(h, r.contention().topResourceHogs());
        h.append("</section>");
    }

    private void slowQueries(StringBuilder h, QueueAnalysisResult r) {
        h.append("<section><h2>Top slow queries</h2>");
        slowQueryTable(h, r.topSlowQueries());
        h.append("</section>");
    }

    private void slowQueryTable(StringBuilder h, java.util.List<QueueAnalysisResult.SlowQueryRef> rows) {
        if (rows.isEmpty()) {
            h.append("<p class=\"muted\">No completed SQL executions.</p>");
            return;
        }
        h.append("<table><thead><tr><th>StatementID</th><th>Execution</th><th>Duration</th>"
                + "<th>Dominant bottleneck</th><th>Contention</th><th>Own core-ms</th></tr></thead><tbody>");
        for (var q : rows) {
            h.append("<tr>");
            td(h, q.statementId() == null ? "-" : esc(q.statementId()));
            td(h, String.valueOf(q.executionId()));
            td(h, duration(q.durationMs()));
            td(h, esc(q.dominantBottleneck()));
            td(h, q.contentionLimited() ? "yes" : "no");
            td(h, String.valueOf(q.ownCoreMs()));
            h.append("</tr>");
        }
        h.append("</tbody></table>");
    }

    private void recommendations(StringBuilder h, QueueAnalysisResult r) {
        h.append("<section><h2>Global recommendations</h2>");
        if (r.globalRecommendations().isEmpty()) {
            h.append("<p class=\"muted\">No queue-level recommendation met the evidence threshold.</p></section>");
            return;
        }
        for (var rec : r.globalRecommendations()) {
            h.append("<div class=\"rec\"><b>").append(esc(rec.queueRuleId())).append("</b> &middot; ")
                    .append(rec.confidence()).append("<br><code>")
                    .append(esc(rec.recommendation().action())).append("</code>")
                    .append("<p>").append(esc(rec.recommendation().rationale())).append("</p>")
                    .append("<p class=\"muted\">Evidence: ").append(esc(rec.evidence()))
                    .append("<br>Coverage: ").append(esc(rec.expectedCoverage())).append("</p></div>");
        }
        h.append("</section>");
    }

    private void embeddedJson(StringBuilder h, QueueAnalysisResult r) throws IOException {
        h.append("<section><h2>Raw queue analysis (JSON contract)</h2><details><summary>Show JSON</summary><pre>")
                .append(esc(jsonWriter.toJson(r))).append("</pre></details></section>");
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

    private static String esc(String s) {
        if (s == null) return "";
        StringBuilder b = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '&' -> b.append("&amp;");
                case '<' -> b.append("&lt;");
                case '>' -> b.append("&gt;");
                case '"' -> b.append("&quot;");
                case '\'' -> b.append("&#39;");
                default -> b.append(c);
            }
        }
        return b.toString();
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
}
