package io.sparkadvisor.ui.live;

import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

/**
 * Driver-side Spark plugin entry point for the live Spark UI integration.
 *
 * <p>Configure with:
 * <pre>{@code
 * spark.plugins=io.sparkadvisor.ui.live.SparkAdvisorSparkPlugin
 * spark.sparkadvisor.live.enabled=true
 * }</pre>
 *
 * <p>The executor side intentionally returns {@code null}; Spark may still instantiate this
 * entry class on executors because {@code spark.plugins} is cluster-wide, but all UI/listener
 * work is driver-only.
 */
public final class SparkAdvisorSparkPlugin implements SparkPlugin {

    @Override
    public DriverPlugin driverPlugin() {
        return new SparkAdvisorDriverPlugin();
    }

    @Override
    public ExecutorPlugin executorPlugin() {
        return null;
    }
}
