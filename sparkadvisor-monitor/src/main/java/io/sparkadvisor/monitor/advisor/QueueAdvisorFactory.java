package io.sparkadvisor.monitor.advisor;

import io.sparkadvisor.advisor.AdvisorFactory;
import io.sparkadvisor.core.util.Strings;

/**
 * Queue-level advisor wiring. Queue reports always include deterministic queue rules; this
 * factory controls only the optional LLM advice block.
 */
public final class QueueAdvisorFactory {

    private QueueAdvisorFactory() {}

    public static QueueLlmAdvisor forMode(String mode) {
        if (Strings.isBlank(mode) || "none".equalsIgnoreCase(mode.trim())) {
            return null;
        }
        String normalized = mode.trim().toLowerCase();
        if (normalized.equals("llm")
                || normalized.equals("minimax")
                || normalized.equals("llm:minimax")
                || normalized.equals("llm:minimax-m2.5")
                || normalized.equals("anthropic")
                || normalized.equals("claude")
                || normalized.equals("llm:anthropic")
                || normalized.equals("llm:claude")) {
            return new QueueLlmAdvisor(AdvisorFactory.llmProviderForMode(normalized));
        }
        return null;
    }
}
