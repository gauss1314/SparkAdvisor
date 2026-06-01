package io.sparkadvisor.ui.live;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.ui.SparkUI;

import java.util.Collections;
import java.util.Map;
import java.util.logging.Logger;

import scala.Option;

/**
 * Driver component that attaches the SparkAdvisor tab to a live Spark UI.
 */
public final class SparkAdvisorDriverPlugin implements DriverPlugin {

    public static final String LIVE_ENABLED = "spark.sparkadvisor.live.enabled";
    public static final String COLLECT_TASK_INTERVALS =
            "spark.sparkadvisor.live.collectTaskIntervals";

    private static final Logger LOG = Logger.getLogger(SparkAdvisorDriverPlugin.class.getName());

    private SparkContext sparkContext;
    private LiveApplicationStore listener;

    @Override
    public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
        SparkConf conf = pluginContext.conf();
        if (!conf.getBoolean(LIVE_ENABLED, false)) {
            LOG.info("SparkAdvisor live UI plugin is loaded but disabled by " + LIVE_ENABLED);
            return Collections.emptyMap();
        }

        this.sparkContext = sc;
        boolean collectTaskIntervals = conf.getBoolean(COLLECT_TASK_INTERVALS, false);
        this.listener = new LiveApplicationStore(collectTaskIntervals);
        // VERIFY@3.5.1: DriverPlugin.init runs before listenerBus.start(), and
        // SparkContext.addSparkListener registers with the shared queue.
        sc.addSparkListener(listener);

        try {
            Option<SparkUI> uiOption = sc.ui(); // VERIFY@3.5.1: SparkContext.ui(): Option[SparkUI]
            if (uiOption.isDefined()) {
                SparkUI ui = uiOption.get();
                ui.attachTab(new LiveSparkAdvisorTab(ui, listener));
                LOG.info("SparkAdvisor live tab attached to driver Spark UI");
            } else {
                LOG.warning("SparkAdvisor live UI is enabled but Spark UI is disabled; "
                        + "no tab will be attached");
            }
        } catch (Throwable t) {
            LOG.warning("Failed to attach SparkAdvisor live tab: " + t);
        }
        return Collections.emptyMap();
    }

    @Override
    public void registerMetrics(String appId, PluginContext pluginContext) {
        LOG.info("SparkAdvisor live UI initialized for app " + appId);
    }

    @Override
    public void shutdown() {
        if (sparkContext != null && listener != null) {
            try {
                sparkContext.removeSparkListener(listener);
            } catch (Throwable t) {
                LOG.fine("Ignoring SparkAdvisor listener removal failure during shutdown: " + t);
            }
        }
    }
}
