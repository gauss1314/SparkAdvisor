package io.sparkadvisor.monitor.advisor;

import io.sparkadvisor.advisor.llm.AdviceResponseParser;
import io.sparkadvisor.advisor.llm.LlmProvider;
import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;
import io.sparkadvisor.report.model.AnalysisResult;

import java.util.List;
import java.util.logging.Logger;

/**
 * LLM-backed queue advisor. It mirrors the single-SQL {@code LlmAdvisor}, but consumes
 * {@link QueueAnalysisResult} instead of a single {@code AnalysisResult}.
 */
public final class QueueLlmAdvisor {

    private static final Logger LOG = Logger.getLogger(QueueLlmAdvisor.class.getName());

    private final LlmProvider provider;
    private final QueuePromptBuilder promptBuilder = new QueuePromptBuilder();
    private final AdviceResponseParser parser = new AdviceResponseParser();

    public QueueLlmAdvisor(LlmProvider provider) {
        this.provider = provider;
    }

    public String name() {
        return provider == null ? "llm:none" : provider.name();
    }

    public AnalysisResult.AiAdvice advise(QueueAnalysisResult result) {
        if (provider == null) {
            return new AnalysisResult.AiAdvice(name(),
                    "No LLM provider configured; set one to generate queue-level AI advice.",
                    new java.util.ArrayList<>());
        }
        try {
            String system = promptBuilder.systemPrompt();
            String user = promptBuilder.userPrompt(result);
            String raw = provider.complete(system, user);
            return parser.parse(name(), raw);
        } catch (Exception e) {
            LOG.warning("Queue LLM advice failed: " + e);
            return new AnalysisResult.AiAdvice(name(),
                    "Queue AI advice unavailable (" + e.getClass().getSimpleName()
                            + "). The deterministic queue findings and recommendations still apply.",
                    new java.util.ArrayList<>());
        }
    }
}
