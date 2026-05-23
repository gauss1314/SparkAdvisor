package io.sparkadvisor.ui.plugin;

import io.sparkadvisor.ui.tab.SparkAdvisorTab;

import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.status.AppHistoryServerPlugin;
import org.apache.spark.status.ElementTrackingStore;
import org.apache.spark.ui.SparkUI;

import java.util.logging.Logger;

import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

/**
 * SparkAdvisor's History Server integration.
 *
 * <p>Implements Spark's official extension point {@link AppHistoryServerPlugin}, which the
 * History Server discovers via {@code ServiceLoader} (see
 * {@code META-INF/services/org.apache.spark.status.AppHistoryServerPlugin}). Dropping the
 * built jar into the SHS classpath is all that's required — no Spark config or launcher
 * changes. This is the same mechanism Spark SQL's own tab uses.
 *
 * <h2>Integration strategy (self-contained — design §11.1, strategy B)</h2>
 * Rather than piggy-backing on the SHS's {@code AppStatusStore}, we:
 * <ul>
 *   <li>{@link #createListeners} returns EMPTY — we do not interfere with SHS replay; and</li>
 *   <li>{@link #setupUI} attaches a tab that re-parses the event log with SparkAdvisor's own
 *       engine (core/analyzer/predictor) and renders the report.</li>
 * </ul>
 * This reuses our fully-tested pipeline and keeps us decoupled from SHS internals. The log is
 * parsed lazily (only when a user opens the tab and queries a StatementID), so the extra
 * parse cost is paid on demand, not at SHS startup.
 *
 * <p>Note: classes here (SparkUI, ElementTrackingStore, AppHistoryServerPlugin) are Spark
 * developer/internal APIs; accessors are marked {@code // VERIFY@3.5.1}.
 */
public final class SparkAdvisorHistoryPlugin implements AppHistoryServerPlugin {

    private static final Logger LOG = Logger.getLogger(SparkAdvisorHistoryPlugin.class.getName());

    @Override
    public Seq<SparkListener> createListeners(SparkConf conf, ElementTrackingStore store) {
        // Strategy B: we do our own parsing in setupUI; no listeners needed here.
        // VERIFY@3.5.1: return type is scala.collection.Seq[SparkListener].
        return CollectionConverters.asScala(new java.util.ArrayList<SparkListener>()).toList();
    }

    @Override
    public int displayOrder() {
        // Place after Spark's built-in tabs (Jobs/Stages/.../SQL).
        return 100;
    }

    @Override
    public void setupUI(SparkUI ui) {
        try {
            SparkAdvisorTab tab = new SparkAdvisorTab(ui);
            // VERIFY@3.5.1: WebUITab.attachPage(...) and SparkUI.attachTab(WebUITab) exist and
            // are accessible from Java; SQLHistoryServerPlugin.setupUI does the same.
            ui.attachTab(tab);
            // Static resources (none required currently; CSS is inlined in the page).
            LOG.info("SparkAdvisor tab attached to History Server UI for app "
                    + safeAppId(ui));
        } catch (Throwable t) {
            // Never let a plugin failure break the whole History UI for an application.
            LOG.warning("Failed to attach SparkAdvisor tab: " + t);
        }
    }

    private static String safeAppId(SparkUI ui) {
        try {
            // VERIFY@3.5.1: SparkUI exposes the appId (Option[String]).
            return ui.appId();
        } catch (Throwable t) {
            return "<unknown>";
        }
    }
}
