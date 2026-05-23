package io.sparkadvisor.advisor.llm;

/**
 * Abstracts a single LLM chat/completion call. Concrete providers (Anthropic, a local model,
 * etc.) implement the transport; the {@code LlmAdvisor} stays provider-agnostic.
 *
 * <p>Implementations encapsulate endpoint, auth, timeout, and retry. They receive a system
 * prompt and a user prompt and return the model's raw text response.
 */
public interface LlmProvider {

    /** Provider identifier for {@code AiAdvice.provider}, e.g. "llm:claude". */
    String name();

    /**
     * Complete a prompt and return the raw text response.
     *
     * @throws Exception on transport/auth/timeout errors; the advisor catches these and
     *                   degrades gracefully.
     */
    String complete(String systemPrompt, String userPrompt) throws Exception;
}
