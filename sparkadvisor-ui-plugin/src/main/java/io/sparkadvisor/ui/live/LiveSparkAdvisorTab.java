package io.sparkadvisor.ui.live;

import io.sparkadvisor.ui.render.SparkUiChrome;

import org.apache.spark.ui.SparkUI;
import org.apache.spark.ui.SparkUITab;

/**
 * SparkAdvisor tab attached directly to a live driver Spark UI.
 */
public final class LiveSparkAdvisorTab extends SparkUITab {

    public static final String PREFIX = "sparkadvisor";

    public LiveSparkAdvisorTab(SparkUI parent, LiveApplicationStore store) {
        super(parent, PREFIX);
        attachPage(new LiveSparkAdvisorPage(this, parent, store));
    }

    @Override
    public String name() {
        return "SparkAdvisor";
    }

    @Override
    public int displayOrder() {
        return SparkUiChrome.LAST_TAB_ORDER;
    }
}
