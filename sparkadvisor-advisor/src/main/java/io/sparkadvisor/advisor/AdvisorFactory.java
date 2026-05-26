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
        String m0 = mode.trim().toLowerCase();
        if ("none".equals(m0)) return null;
        if ("llm".equals(m0) || "minimax".equals(m0) || "llm:minimax".equals(m0) || "llm:minimax-m2.5".equals(m0)
                || "anthropic".equals(m0) || "claude".equals(m0) || "llm:anthropic".equals(m0) || "llm:claude".equals(m0)) {
            return new LlmAdvisor(llmProviderForMode(mode));
        }
        return new RuleBasedAdvisor();
    }

    /**
     * Resolve the LLM provider for an LLM mode. Plain {@code llm} intentionally defaults to
     * MiniMax-M2.5.
     */
    public static LlmProvider llmProviderForMode(String mode) {
        String m = mode == null ? "llm" : mode.trim().toLowerCase();
        if ("anthropic".equals(m) || "claude".equals(m) || "llm:anthropic".equals(m) || "llm:claude".equals(m)) {
            return new AnthropicLlmProvider();
        }
        return new MinimaxLlmProvider();
    }
}
