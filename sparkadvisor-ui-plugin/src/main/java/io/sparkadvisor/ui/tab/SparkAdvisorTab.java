package io.sparkadvisor.ui.tab;

import io.sparkadvisor.ui.render.SparkUiChrome;

import org.apache.spark.ui.SparkUI;
import org.apache.spark.ui.SparkUITab;

/**
 * The "SparkAdvisor" tab in the History Server UI. Registers a single page that, given a
 * StatementID, renders the analysis report for the corresponding SQL.
 *
 * <p>{@code SparkUITab(parent, prefix)} sets the URL prefix; the tab appears in the UI nav bar
 * with this name. VERIFY@3.5.1: SparkUITab constructor signature is (SparkUI, String) and
 * attachPage(WebUIPage) is accessible.
 */
public final class SparkAdvisorTab extends SparkUITab {

    public static final String PREFIX = "sparkadvisor";

    private final SparkUI parent;

    public SparkAdvisorTab(SparkUI parent) {
        super(parent, PREFIX);
        this.parent = parent;
        attachPage(new SparkAdvisorPage(this, parent));
    }

    /** Tab display name shown in the UI nav bar. VERIFY@3.5.1: name() is overridable. */
    @Override
    public String name() {
        return "SparkAdvisor";
    }

    @Override
    public int displayOrder() {
        return SparkUiChrome.LAST_TAB_ORDER;
    }

    public SparkUI parentUI() {
        return parent;
    }
}
