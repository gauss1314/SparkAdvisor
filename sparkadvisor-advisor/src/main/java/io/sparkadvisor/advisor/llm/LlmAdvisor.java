package io.sparkadvisor.advisor.llm;

import io.sparkadvisor.advisor.api.TuningAdvisor;
import io.sparkadvisor.report.i18n.ReportLanguage;
import io.sparkadvisor.report.model.AnalysisResult;

import java.util.List;
import java.util.logging.Logger;

/**
 * LLM-backed advisor. Orchestration:
 * <ol>
 *   <li>{@link PromptBuilder} turns the structured {@link AnalysisResult} into system+user
 *       prompts — feeding the model the KB-scale summary, never the raw log.</li>
 *   <li>An {@link LlmProvider} performs the call.</li>
 *   <li>{@link AdviceResponseParser} parses the JSON response into {@code AiAdvice}.</li>
 * </ol>
 *
 * <p>Robust by contract: any failure (no provider, transport error, bad response) degrades to
 * a non-null {@code AiAdvice} explaining the failure, so report generation never breaks.
 */
public final class LlmAdvisor implements TuningAdvisor {

    private static final Logger LOG = Logger.getLogger(LlmAdvisor.class.getName());

    private final LlmProvider provider;
    private final ReportLanguage language;
    private final PromptBuilder promptBuilder;
    private final AdviceResponseParser parser = new AdviceResponseParser();

    public LlmAdvisor(LlmProvider provider) {
        this(provider, ReportLanguage.EN);
    }

    public LlmAdvisor(LlmProvider provider, ReportLanguage language) {
        this.provider = provider;
        this.language = language == null ? ReportLanguage.EN : language;
        this.promptBuilder = new PromptBuilder(this.language);
    }

    @Override
    public String name() {
        return provider == null ? "llm:none" : provider.name();
    }

    @Override
    public AnalysisResult.AiAdvice advise(AnalysisResult result) {
        if (provider == null) {
            return new AnalysisResult.AiAdvice(name(),
                    language.isChinese()
                            ? "未配置 LLM provider；配置后才能生成 AI 调优建议。"
                            : "No LLM provider configured; set one to generate AI advice.",
                    new java.util.ArrayList<io.sparkadvisor.core.finding.Recommendation>());
        }
        try {
            String system = promptBuilder.systemPrompt();
            String user = promptBuilder.userPrompt(result);
            String raw = provider.complete(system, user);
            return parser.parse(name(), raw);
        } catch (Exception e) {
            LOG.warning("LLM advice failed: " + e);
            return new AnalysisResult.AiAdvice(name(),
                    language.isChinese()
                            ? "AI 调优建议不可用（" + e.getClass().getSimpleName()
                            + "）。上方的规则发现和预测仍然有效。"
                            : "AI advice unavailable (" + e.getClass().getSimpleName()
                            + "). The rule-based findings and predictions above still apply.",
                    new java.util.ArrayList<io.sparkadvisor.core.finding.Recommendation>());
        }
    }
}
