package io.sparkadvisor.ui.live;

import io.sparkadvisor.core.util.Strings;
import io.sparkadvisor.monitor.QueueAnalyzer;
import io.sparkadvisor.report.i18n.ReportLanguage;

import org.apache.spark.ui.SparkUI;
import org.apache.spark.ui.WebUIPage;

import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.xml.Node;
import scala.xml.NodeSeq;
import scala.xml.Unparsed;

/**
 * Live SparkAdvisor page backed by the driver's in-memory listener snapshot.
 */
public final class LiveSparkAdvisorPage extends WebUIPage {

    private static final Logger LOG = Logger.getLogger(LiveSparkAdvisorPage.class.getName());

    private final SparkUI ui;
    private final LiveAnalysisCoordinator coordinator;

    public LiveSparkAdvisorPage(SparkUI ui, LiveApplicationStore store) {
        super("");
        this.ui = ui;
        this.coordinator = new LiveAnalysisCoordinator(store);
    }

    @Override
    public Seq<Node> render(HttpServletRequest request) {
        String statementId = request.getParameter("statementId");
        ReportLanguage language = resolveLanguage(request);
        int queueTop = intParam(request, "top", QueueAnalyzer.DEFAULT_TOP_N, 1, 500);
        int samplePerStratum = intParam(request, "samplePerStratum",
                QueueAnalyzer.DEFAULT_SAMPLE_PER_STRATUM, 0, 100);
        String bucketValue = Strings.isBlank(request.getParameter("bucket"))
                ? "1h"
                : request.getParameter("bucket");
        long bucketMs = parseDurationMs(bucketValue);

        String bodyHtml;
        try {
            String css = coordinator.stylesheet();
            String form = searchForm(statementId, language, queueTop, samplePerStratum, bucketValue);
            String report;
            if (Strings.isBlank(statementId)) {
                css = css + "\n" + coordinator.queueStylesheet();
                report = coordinator.renderQueueBody(queueTop, samplePerStratum, bucketMs, language);
            } else {
                report = coordinator.renderSqlBody(statementId, language);
            }
            bodyHtml = "<style>" + css + "</style>"
                    + "<div class=\"sparkadvisor-root\">" + form + report + "</div>";
        } catch (Throwable t) {
            LOG.warning("SparkAdvisor live analysis failed: " + t);
            bodyHtml = "<div class=\"banner warn\">Could not analyze live driver snapshot: "
                    + escapeText(String.valueOf(t.getMessage())) + "</div>";
        }
        return nodeSeq(new Unparsed(bodyHtml));
    }

    private String searchForm(String current, ReportLanguage language,
                              int queueTop, int samplePerStratum, String bucket) {
        String val = current == null ? "" : escapeAttr(current);
        boolean zh = language != null && language.isChinese();
        String lang = language == null ? "en" : (language.isChinese() ? "zh" : "en");
        return "<div style=\"padding:12px 0\">"
                + "<form method=\"get\">"
                + "<label style=\"font-size:13px;color:#8b949e\">StatementID&nbsp;</label>"
                + "<input type=\"text\" name=\"statementId\" value=\"" + val + "\" "
                + "placeholder=\"e.g. 20260521_abc123\" style=\"padding:4px 8px;width:280px\"/> "
                + "<select name=\"lang\" style=\"padding:4px 8px\">"
                + "<option value=\"zh\"" + ("zh".equals(lang) ? " selected" : "") + ">中文</option>"
                + "<option value=\"en\"" + ("en".equals(lang) ? " selected" : "") + ">English</option>"
                + "</select> "
                + "<label style=\"font-size:13px;color:#8b949e;margin-left:8px\">"
                + (zh ? "最慢数" : "Top") + "&nbsp;</label>"
                + "<input type=\"number\" name=\"top\" min=\"1\" max=\"500\" value=\""
                + queueTop + "\" style=\"padding:4px 8px;width:70px\"/> "
                + "<label style=\"font-size:13px;color:#8b949e\">"
                + (zh ? "分层样本" : "Samples") + "&nbsp;</label>"
                + "<input type=\"number\" name=\"samplePerStratum\" min=\"0\" max=\"100\" value=\""
                + samplePerStratum + "\" style=\"padding:4px 8px;width:70px\"/> "
                + "<label style=\"font-size:13px;color:#8b949e\">"
                + (zh ? "分桶" : "Bucket") + "&nbsp;</label>"
                + "<input type=\"text\" name=\"bucket\" value=\"" + escapeAttr(bucket)
                + "\" style=\"padding:4px 8px;width:70px\"/> "
                + "<button type=\"submit\" style=\"padding:4px 12px\">"
                + (zh ? "分析 SQL" : "Analyze SQL") + "</button>"
                + "<span style=\"margin-left:10px;color:#8b949e;font-size:12px\">"
                + (zh ? "留空查看当前 Driver 快照" : "leave blank for the current driver snapshot")
                + "</span>"
                + "</form></div>";
    }

    private static int intParam(HttpServletRequest request, String name, int fallback, int min, int max) {
        String raw = request.getParameter(name);
        if (Strings.isBlank(raw)) {
            return fallback;
        }
        try {
            int parsed = Integer.parseInt(raw.trim());
            return Math.max(min, Math.min(max, parsed));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static long parseDurationMs(String value) {
        if (Strings.isBlank(value)) {
            return QueueAnalyzer.DEFAULT_BUCKET_MS;
        }
        String v = value.trim().toLowerCase(java.util.Locale.ROOT);
        long multiplier = 1L;
        if (v.endsWith("ms")) {
            v = v.substring(0, v.length() - 2);
        } else if (v.endsWith("s")) {
            multiplier = 1000L;
            v = v.substring(0, v.length() - 1);
        } else if (v.endsWith("m")) {
            multiplier = 60_000L;
            v = v.substring(0, v.length() - 1);
        } else if (v.endsWith("h")) {
            multiplier = 60L * 60L * 1000L;
            v = v.substring(0, v.length() - 1);
        }
        try {
            return Math.max(60_000L, Long.parseLong(v.trim()) * multiplier);
        } catch (NumberFormatException e) {
            return QueueAnalyzer.DEFAULT_BUCKET_MS;
        }
    }

    private ReportLanguage resolveLanguage(HttpServletRequest request) {
        String configured = "auto";
        try {
            configured = ui.conf().get("spark.sparkadvisor.lang", "auto"); // VERIFY@3.5.1
        } catch (Throwable t) {
            configured = "auto";
        }
        return ReportLanguage.resolveForUi(
                request.getParameter("lang"),
                configured,
                request.getHeader("Accept-Language"));
    }

    @SuppressWarnings("unchecked")
    private static Seq<Node> nodeSeq(Node n) {
        // VERIFY@3.5.1: building a single-element scala Seq[Node] from Java.
        return (Seq<Node>) (Seq<?>) NodeSeq.fromSeq(
                JavaConverters.asScalaBuffer(new java.util.ArrayList<scala.xml.Node>(
                        java.util.Arrays.asList(n))).toSeq());
    }

    private static String escapeAttr(String s) {
        return s.replace("&", "&amp;").replace("\"", "&quot;").replace("<", "&lt;");
    }

    private static String escapeText(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
