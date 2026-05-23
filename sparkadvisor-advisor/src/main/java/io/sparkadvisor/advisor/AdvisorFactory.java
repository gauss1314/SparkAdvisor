package io.sparkadvisor.advisor;

import io.sparkadvisor.advisor.api.TuningAdvisor;
import io.sparkadvisor.advisor.llm.AnthropicLlmProvider;
import io.sparkadvisor.advisor.llm.LlmAdvisor;
import io.sparkadvisor.advisor.llm.LlmProvider;
import io.sparkadvisor.advisor.llm.MinimaxLlmProvider;
import io.sparkadvisor.advisor.rule.RuleBasedAdvisor;

/**
 * Selects a {@link TuningAdvisor} by mode. Keeps advisor wiring in one place so the CLI/UI
 * just pass a mode string.
 */
public final class AdvisorFactory {

    private AdvisorFactory() {}

    /**
     * @param mode "none" (no advice), "rule" (default, offline), or "llm" (MiniMax-backed)
     * @return the advisor, or null for "none"
     */
    public static TuningAdvisor forMode(String mode) {
        if (mode == null) {
            return new RuleBasedAdvisor();
        }
        return switch (mode.trim().toLowerCase()) {
            case "none" -> null;
            case "llm", "minimax", "llm:minimax", "llm:minimax-m2.5",
                    "anthropic", "claude", "llm:anthropic", "llm:claude" ->
                    new LlmAdvisor(llmProviderForMode(mode));
            default -> new RuleBasedAdvisor();
        };
    }

    /**
     * Resolve the LLM provider for an LLM mode. Plain {@code llm} intentionally defaults to
     * MiniMax-M2.5.
     */
    public static LlmProvider llmProviderForMode(String mode) {
        String m = mode == null ? "llm" : mode.trim().toLowerCase();
        return switch (m) {
            case "anthropic", "claude", "llm:anthropic", "llm:claude" ->
                    new AnthropicLlmProvider();
            default -> new MinimaxLlmProvider();
        };
    }
}
