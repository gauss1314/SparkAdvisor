package io.sparkadvisor.ui.tab;

import io.sparkadvisor.core.util.Strings;
import io.sparkadvisor.ui.render.AnalysisCoordinator;
import io.sparkadvisor.ui.render.EventLogPathResolver;
import io.sparkadvisor.ui.render.QueueAnalysisCoordinator;
import io.sparkadvisor.monitor.QueueAnalyzer;

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

    private final SparkUI ui;
    private final AnalysisCoordinator coordinator;
    private final QueueAnalysisCoordinator queueCoordinator;
    private final EventLogPathResolver pathResolver;

    public SparkAdvisorPage(SparkAdvisorTab parent, SparkUI ui) {
        super(""); // empty prefix => page lives at /sparkadvisor
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
        String appId = appId();
        String path = pathResolver.pathFor(appId);

        String bodyHtml;
        try {
            String css = coordinator.stylesheet();
            String form = searchForm(statementId);
            String report;
            if (Strings.isBlank(statementId)) {
                css = css + "\n" + queueCoordinator.stylesheet();
                report = queueCoordinator.renderBody(
                        path, QueueAnalyzer.DEFAULT_TOP_N, QueueAnalyzer.DEFAULT_BUCKET_MS);
            } else {
                report = coordinator.renderBody(path, statementId);
            }
            bodyHtml = "<style>" + css + "</style>"
                    + "<div class=\"sparkadvisor-root\">" + form + report + "</div>";
        } catch (Throwable t) {
            LOG.warning("SparkAdvisor analysis failed for " + path + ": " + t);
            bodyHtml = errorHtml(path, t);
        }

        // Emit our raw HTML verbatim as a single XML node.
        //
        // NOTE: We deliberately do NOT call UIUtils.headerSparkPage here. That helper's
        // signature has shifted across Spark versions and is the most brittle internal API to
        // bind from Java. Returning our self-contained content (with inlined CSS) renders the
        // tab body without Spark's standard page header frame, which is an acceptable tradeoff
        // for robustness. If the standard frame is desired later, wrap this content with the
        // version-correct headerSparkPage overload (VERIFY@3.5.1) behind a feature check.
        return nodeSeq(new Unparsed(bodyHtml));
    }

    private String searchForm(String current) {
        String val = current == null ? "" : escapeAttr(current);
        return "<div style=\"padding:12px 0\">"
                + "<form method=\"get\">"
                + "<label style=\"font-size:13px;color:#8b949e\">StatementID&nbsp;</label>"
                + "<input type=\"text\" name=\"statementId\" value=\"" + val + "\" "
                + "placeholder=\"e.g. 20260521_abc123\" style=\"padding:4px 8px;width:280px\"/> "
                + "<button type=\"submit\" style=\"padding:4px 12px\">Analyze SQL</button>"
                + "<span style=\"margin-left:10px;color:#8b949e;font-size:12px\">"
                + "leave blank for the queue report</span>"
                + "</form></div>";
    }

    private String errorHtml(String path, Throwable t) {
        return "<div class=\"banner warn\">Could not analyze event log at <code>"
                + escapeText(path) + "</code>: " + escapeText(String.valueOf(t.getMessage()))
                + "</div>";
    }

    private String appId() {
        try {
            return ui.appId(); // VERIFY@3.5.1
        } catch (Throwable t) {
            return "";
        }
    }

    // --- scala.xml helpers ------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static Seq<Node> nodeSeq(Node n) {
        // VERIFY@3.5.1: building a single-element scala Seq[Node] from Java.
        return (Seq<Node>) (Seq<?>) NodeSeq.fromSeq(
                JavaConverters.asScalaBuffer(new java.util.ArrayList<scala.xml.Node>(java.util.Arrays.asList(n))).toSeq());
    }

    private static String escapeAttr(String s) {
        return s.replace("&", "&amp;").replace("\"", "&quot;").replace("<", "&lt;");
    }

    private static String escapeText(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
