package io.sparkadvisor.ui.render;

import org.apache.spark.ui.SparkUITab;
import org.apache.spark.ui.UIUtils;

import javax.servlet.http.HttpServletRequest;

import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;
import scala.xml.Node;
import scala.xml.NodeSeq;
import scala.xml.Unparsed;

/**
 * Small compatibility wrapper around Spark's standard UI chrome.
 */
public final class SparkUiChrome {

    public static final int LAST_TAB_ORDER = Integer.MAX_VALUE;

    private SparkUiChrome() {}

    public static Seq<Node> page(final HttpServletRequest request, String title,
                                 SparkUITab activeTab, final String bodyHtml) {
        // VERIFY@3.5.1: UIUtils.headerSparkPage(request,title,content,activeTab,helpText,
        // showVisualization,useDataTables) returns the standard Spark UI page with nav tabs.
        return UIUtils.headerSparkPage(
                request,
                title,
                new AbstractFunction0<Seq<Node>>() {
                    @Override
                    public Seq<Node> apply() {
                        return nodeSeq(new Unparsed(bodyHtml));
                    }
                },
                activeTab,
                Option.<String>empty(),
                false,
                false);
    }

    public static String wrap(String contentHtml) {
        return "<style>" + stylesheet() + "</style>"
                + "<div class=\"sparkadvisor-root\">" + contentHtml + "</div>";
    }

    public static String stylesheet() {
        return String.join("\n",
                ".sparkadvisor-root{--sa-accent:#1f77b4;--sa-line:#dee2e6;--sa-muted:#6c757d;",
                "  --sa-warn:#f0ad4e;--sa-crit:#d9534f;--sa-ok:#5cb85c;color:#212529;font-size:14px}",
                ".sparkadvisor-root .sparkadvisor-control{background:#f8f9fa;border:1px solid var(--sa-line);",
                "  border-radius:4px;padding:12px;margin:0 0 16px}",
                ".sparkadvisor-root .sparkadvisor-statement{width:280px}",
                ".sparkadvisor-root .sparkadvisor-small{width:78px}",
                ".sparkadvisor-root header{padding:0 0 12px;margin:0 0 12px;border-bottom:1px solid var(--sa-line)}",
                ".sparkadvisor-root header h1{margin:0;font-size:22px;font-weight:500;color:#343a40}",
                ".sparkadvisor-root h2{font-size:18px;margin:0 0 12px;color:#343a40;font-weight:500}",
                ".sparkadvisor-root h3{font-size:15px;margin:12px 0 8px;color:#343a40;font-weight:500}",
                ".sparkadvisor-root section{padding:16px 0;border-bottom:1px solid #f0f0f0}",
                ".sparkadvisor-root .sub,.sparkadvisor-root .muted{color:var(--sa-muted)}",
                ".sparkadvisor-root .banner{padding:10px 12px;margin-bottom:12px;border:1px solid var(--sa-line);",
                "  border-radius:4px;background:#f8f9fa}",
                ".sparkadvisor-root .banner.warn{background:#fff3cd;border-color:#ffeeba;color:#856404}",
                ".sparkadvisor-root .grid{display:flex;flex-wrap:wrap;gap:10px 28px}",
                ".sparkadvisor-root .kv{display:flex;flex-direction:column}",
                ".sparkadvisor-root .kv .k,.sparkadvisor-root .k{color:var(--sa-muted);font-size:12px}",
                ".sparkadvisor-root .kv .v,.sparkadvisor-root .v{font-size:15px;font-weight:600;color:#212529}",
                ".sparkadvisor-root .cards{display:flex;flex-wrap:wrap;gap:12px}",
                ".sparkadvisor-root .card{background:#fff;border:1px solid var(--sa-line);border-radius:4px;",
                "  padding:12px 14px;min-width:145px;box-shadow:none}",
                ".sparkadvisor-root .card.warn{border-color:var(--sa-warn);background:#fffaf0}",
                ".sparkadvisor-root .card-v{font-size:20px;font-weight:600;color:#212529}",
                ".sparkadvisor-root .card-l{color:var(--sa-muted);font-size:12px}",
                ".sparkadvisor-root .card-v .diag-v{display:block;font-size:13px;line-height:1.35;font-weight:600}",
                ".sparkadvisor-root table{width:100%;border-collapse:collapse;font-size:13px;background:#fff}",
                ".sparkadvisor-root th,.sparkadvisor-root td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--sa-line)}",
                ".sparkadvisor-root th{color:#343a40;font-weight:600;background:#f8f9fa}",
                ".sparkadvisor-root tr.row-warn td{background:#fff5f5}",
                ".sparkadvisor-root pre{background:#f8f9fa;border:1px solid var(--sa-line);border-radius:4px;",
                "  padding:12px;overflow:auto;font-size:12px;max-height:420px;color:#212529}",
                ".sparkadvisor-root pre.sql{white-space:pre-wrap}",
                ".sparkadvisor-root details summary{cursor:pointer;color:var(--sa-accent);margin:6px 0}",
                ".sparkadvisor-root code{background:#f1f3f5;color:#343a40;padding:1px 5px;border-radius:3px;font-size:12px}",
                ".sparkadvisor-root .bars,.sparkadvisor-root .chart{max-width:100%;height:auto;display:block}",
                ".sparkadvisor-root .bar-label,.sparkadvisor-root .chart-label{fill:var(--sa-muted)}",
                ".sparkadvisor-root .bar-val{fill:#212529}",
                ".sparkadvisor-root .bar-actual{fill:var(--sa-crit)}",
                ".sparkadvisor-root .bar-critical{fill:#337ab7}",
                ".sparkadvisor-root .bar-ideal{fill:var(--sa-ok)}",
                ".sparkadvisor-root .axis{stroke:var(--sa-line);stroke-width:1}",
                ".sparkadvisor-root .line-p50{fill:none;stroke:var(--sa-ok);stroke-width:2}",
                ".sparkadvisor-root .line-p95{fill:none;stroke:#337ab7;stroke-width:2}",
                ".sparkadvisor-root .line-p99{fill:none;stroke:var(--sa-warn);stroke-width:2}",
                ".sparkadvisor-root .line-util{fill:none;stroke:var(--sa-crit);stroke-width:2;stroke-dasharray:5 4}",
                ".sparkadvisor-root .legend{display:flex;flex-wrap:wrap;gap:14px;margin:0 0 10px;color:var(--sa-muted);font-size:12px}",
                ".sparkadvisor-root .legend b{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px}",
                ".sparkadvisor-root .bar{height:10px;background:#e9ecef;border-radius:4px;overflow:hidden}",
                ".sparkadvisor-root .bar>span{display:block;height:100%;background:var(--sa-accent)}",
                ".sparkadvisor-root .finding,.sparkadvisor-root .pred,.sparkadvisor-root .rec{background:#fff;",
                "  border:1px solid var(--sa-line);border-left:4px solid var(--sa-accent);border-radius:4px;",
                "  padding:10px 14px;margin-bottom:10px}",
                ".sparkadvisor-root .finding.critical{border-left-color:var(--sa-crit)}",
                ".sparkadvisor-root .finding.warn{border-left-color:var(--sa-warn)}",
                ".sparkadvisor-root .finding.info{border-left-color:var(--sa-accent)}",
                ".sparkadvisor-root .finding-head{margin-bottom:6px}",
                ".sparkadvisor-root .sev{font-size:11px;font-weight:700;padding:1px 6px;border-radius:3px}",
                ".sparkadvisor-root .sev.critical{background:var(--sa-crit);color:#fff}",
                ".sparkadvisor-root .sev.warn{background:var(--sa-warn);color:#212529}",
                ".sparkadvisor-root .sev.info{background:var(--sa-accent);color:#fff}",
                ".sparkadvisor-root .pred h3{font-size:15px;margin:0 0 6px;color:#343a40}",
                ".sparkadvisor-root ul.assume{margin:6px 0;padding-left:18px;color:var(--sa-muted);font-size:12px}",
                ".sparkadvisor-root .query-link{color:var(--sa-accent);text-decoration:none}",
                ".sparkadvisor-root .query-link:hover{text-decoration:underline}",
                ".sparkadvisor-root .share{min-width:110px}");
    }

    @SuppressWarnings("unchecked")
    private static Seq<Node> nodeSeq(Node n) {
        return (Seq<Node>) (Seq<?>) NodeSeq.fromSeq(
                JavaConverters.asScalaBuffer(new java.util.ArrayList<scala.xml.Node>(
                        java.util.Arrays.asList(n))).toSeq());
    }
}
