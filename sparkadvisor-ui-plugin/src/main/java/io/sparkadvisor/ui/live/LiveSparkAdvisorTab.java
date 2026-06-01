package io.sparkadvisor.ui.live;

import org.apache.spark.ui.SparkUI;
import org.apache.spark.ui.WebUITab;

/**
 * SparkAdvisor tab attached directly to a live driver Spark UI.
 */
public final class LiveSparkAdvisorTab extends WebUITab {

    public static final String PREFIX = "sparkadvisor";

    public LiveSparkAdvisorTab(SparkUI parent, LiveApplicationStore store) {
        super(parent, PREFIX);
        attachPage(new LiveSparkAdvisorPage(parent, store));
    }

    @Override
    public String name() {
        return "SparkAdvisor";
    }

    @Override
    public int displayOrder() {
        return 100;
    }
}
