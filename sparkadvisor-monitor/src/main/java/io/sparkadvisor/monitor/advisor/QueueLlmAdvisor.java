package io.sparkadvisor.monitor.advisor;

import io.sparkadvisor.advisor.llm.AdviceResponseParser;
import io.sparkadvisor.advisor.llm.LlmProvider;
import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;
import io.sparkadvisor.report.i18n.ReportLanguage;
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
    private final ReportLanguage language;
    private final QueuePromptBuilder promptBuilder;
    private final AdviceResponseParser parser = new AdviceResponseParser();

    public QueueLlmAdvisor(LlmProvider provider) {
        this(provider, ReportLanguage.EN);
    }

    public QueueLlmAdvisor(LlmProvider provider, ReportLanguage language) {
        this.provider = provider;
        this.language = language == null ? ReportLanguage.EN : language;
        this.promptBuilder = new QueuePromptBuilder(this.language);
    }

    public String name() {
        return provider == null ? "llm:none" : provider.name();
    }

    public AnalysisResult.AiAdvice advise(QueueAnalysisResult result) {
        if (provider == null) {
            return new AnalysisResult.AiAdvice(name(),
                    language.isChinese()
                            ? "未配置 LLM provider；配置后才能生成队列级 AI 建议。"
                            : "No LLM provider configured; set one to generate queue-level AI advice.",
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
                    language.isChinese()
                            ? "队列级 AI 建议不可用（" + e.getClass().getSimpleName()
                            + "）。确定性队列发现和建议仍然有效。"
                            : "Queue AI advice unavailable (" + e.getClass().getSimpleName()
                            + "). The deterministic queue findings and recommendations still apply.",
                    new java.util.ArrayList<>());
        }
    }
}
