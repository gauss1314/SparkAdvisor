package io.sparkadvisor.ui.tab;

import org.apache.spark.ui.SparkUI;
import org.apache.spark.ui.WebUITab;

/**
 * The "SparkAdvisor" tab in the History Server UI. Registers a single page that, given a
 * StatementID, renders the analysis report for the corresponding SQL.
 *
 * <p>{@code WebUITab(parent, prefix)} sets the URL prefix; the tab appears in the UI nav bar
 * with this name. VERIFY@3.5.1: WebUITab constructor signature is (WebUI, String) and
 * attachPage(WebUIPage) is accessible.
 */
public final class SparkAdvisorTab extends WebUITab {

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

    public SparkUI parentUI() {
        return parent;
    }
}
