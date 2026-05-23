package io.sparkadvisor.advisor;

import io.sparkadvisor.advisor.api.TuningAdvisor;
import io.sparkadvisor.advisor.llm.AnthropicLlmProvider;
import io.sparkadvisor.advisor.llm.LlmAdvisor;
import io.sparkadvisor.advisor.rule.RuleBasedAdvisor;

/**
 * Selects a {@link TuningAdvisor} by mode. Keeps advisor wiring in one place so the CLI/UI
 * just pass a mode string.
 */
public final class AdvisorFactory {

    private AdvisorFactory() {}

    /**
     * @param mode "none" (no advice), "rule" (default, offline), or "llm" (LLM-backed)
     * @return the advisor, or null for "none"
     */
    public static TuningAdvisor forMode(String mode) {
        if (mode == null) {
            return new RuleBasedAdvisor();
        }
        return switch (mode.trim().toLowerCase()) {
            case "none" -> null;
            case "llm" -> new LlmAdvisor(new AnthropicLlmProvider());
            default -> new RuleBasedAdvisor();
        };
    }
}
