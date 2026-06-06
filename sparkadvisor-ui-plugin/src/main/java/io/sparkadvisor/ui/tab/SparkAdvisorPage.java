package io.sparkadvisor.ui.tab;

import io.sparkadvisor.core.util.Strings;
import io.sparkadvisor.report.i18n.ReportLanguage;
import io.sparkadvisor.ui.render.AnalysisCoordinator;
import io.sparkadvisor.ui.render.EventLogPathResolver;
import io.sparkadvisor.ui.render.QueueAnalysisCoordinator;
import io.sparkadvisor.ui.render.HistoryRequestPath;
import io.sparkadvisor.ui.render.SparkUiChrome;
import io.sparkadvisor.monitor.QueueAnalyzer;

import org.apache.spark.ui.SparkUI;
import org.apache.spark.ui.WebUIPage;

import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

import scala.collection.Seq;
import scala.xml.Node;

/**
 * The single page under the SparkAdvisor tab. Accepts a {@code statementId} query parameter,
 * runs either queue-level or single-SQL analysis, and renders the report as raw tab content.
 *
 * <p>{@link WebUIPage#render} returns a {@code scala.xml.Seq[Node]}. We build our HTML as a
 * String and wrap it with {@link Unparsed} so Spark emits it verbatim.
 *
 * <p>VERIFY@3.5.1: WebUIPage(prefix:String) constructor; render returns scala.xml.Seq[Node].
 * These are Spark internal UI APIs.
 */
public final class SparkAdvisorPage extends WebUIPage {

    private static final Logger LOG = Logger.getLogger(SparkAdvisorPage.class.getName());

    private final SparkAdvisorTab tab;
    private final SparkUI ui;
    private final AnalysisCoordinator coordinator;
    private final QueueAnalysisCoordinator queueCoordinator;
    private final EventLogPathResolver pathResolver;

    public SparkAdvisorPage(SparkAdvisorTab parent, SparkUI ui) {
        super(""); // empty prefix => page lives at /sparkadvisor
        this.tab = parent;
        this.ui = ui;
        // Hadoop conf inherits the SHS process environment (Kerberos ticket already present).
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        this.coordinator = new AnalysisCoordinator(hadoopConf);
        this.queueCoordinator = new QueueAnalysisCoordinator(hadoopConf);
        // VERIFY@3.5.1: SparkUI.conf() returns the SparkConf.
        this.pathResolver = new EventLogPathResolver(ui.conf());
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
        HistoryRequestPath historyPath = HistoryRequestPath.fromRequestUri(request.getRequestURI());
        String appId = appId();
        if (Strings.isBlank(appId) && historyPath.hasAppId()) {
            appId = historyPath.appId();
        }
        String path = pathResolver.pathFor(appId, historyPath.attemptId());

        String bodyHtml;
        try {
            String form = searchForm(statementId, language, queueTop, samplePerStratum, bucketValue);
            String report;
            if (Strings.isBlank(statementId)) {
                report = queueCoordinator.renderBody(
                        path, queueTop, samplePerStratum, bucketMs, language);
            } else {
                report = coordinator.renderBody(path, statementId, language);
            }
            bodyHtml = SparkUiChrome.wrap(form + report);
        } catch (Throwable t) {
            LOG.warning("SparkAdvisor analysis failed for " + path + ": " + t);
            bodyHtml = SparkUiChrome.wrap(errorHtml(path, t));
        }

        return SparkUiChrome.page(request, "SparkAdvisor", tab, bodyHtml);
    }

    private String searchForm(String current, ReportLanguage language,
                              int queueTop, int samplePerStratum, String bucket) {
        String val = current == null ? "" : escapeAttr(current);
        boolean zh = language != null && language.isChinese();
        String lang = language == null ? "en" : (language.isChinese() ? "zh" : "en");
        return "<div class=\"sparkadvisor-control\">"
                + "<form class=\"form-inline\" method=\"get\">"
                + "<label class=\"mr-2 mb-2\">StatementID</label>"
                + "<input type=\"text\" name=\"statementId\" value=\"" + val + "\" "
                + "placeholder=\"e.g. 20260521_abc123\" "
                + "class=\"form-control form-control-sm sparkadvisor-statement mr-2 mb-2\"/> "
                + "<select name=\"lang\" class=\"form-control form-control-sm mr-2 mb-2\">"
                + "<option value=\"zh\"" + ("zh".equals(lang) ? " selected" : "") + ">中文</option>"
                + "<option value=\"en\"" + ("en".equals(lang) ? " selected" : "") + ">English</option>"
                + "</select> "
                + "<label class=\"mr-2 mb-2\">" + (zh ? "最慢数" : "Top") + "</label>"
                + "<input type=\"number\" name=\"top\" min=\"1\" max=\"500\" value=\""
                + queueTop + "\" class=\"form-control form-control-sm sparkadvisor-small mr-2 mb-2\"/> "
                + "<label class=\"mr-2 mb-2\">" + (zh ? "分层样本" : "Samples") + "</label>"
                + "<input type=\"number\" name=\"samplePerStratum\" min=\"0\" max=\"100\" value=\""
                + samplePerStratum + "\" class=\"form-control form-control-sm sparkadvisor-small mr-2 mb-2\"/> "
                + "<label class=\"mr-2 mb-2\">" + (zh ? "分桶" : "Bucket") + "</label>"
                + "<input type=\"text\" name=\"bucket\" value=\"" + escapeAttr(bucket)
                + "\" class=\"form-control form-control-sm sparkadvisor-small mr-2 mb-2\"/> "
                + "<button type=\"submit\" class=\"btn btn-primary btn-sm mb-2\">"
                + (zh ? "分析 SQL" : "Analyze SQL") + "</button>"
                + "<span class=\"text-muted small ml-2 mb-2\">"
                + (zh ? "留空查看队列报告" : "leave blank for the queue report") + "</span>"
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

    private String errorHtml(String path, Throwable t) {
        return "<div class=\"banner warn\">Could not analyze event log at <code>"
                + escapeText(path) + "</code>: " + escapeText(String.valueOf(t.getMessage()))
                + "</div>";
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

    private String appId() {
        try {
            String value = ui.appId(); // VERIFY@3.5.1
            return Strings.isBlank(value) ? "" : value;
        } catch (Throwable t) {
            return "";
        }
    }

    private static String escapeAttr(String s) {
        return s.replace("&", "&amp;").replace("\"", "&quot;").replace("<", "&lt;");
    }

    private static String escapeText(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
