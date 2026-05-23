package io.sparkadvisor.advisor.api;

import io.sparkadvisor.report.model.AnalysisResult;

/**
 * Produces tuning advice from a completed {@link AnalysisResult}.
 *
 * <p>Design invariant (the whole point of SparkAdvisor's architecture): an advisor consumes
 * the <b>structured</b> {@code AnalysisResult} — already-computed hard metrics, findings, and
 * predictions — and NEVER the raw event log. The result object is the KB-scale, deterministic
 * summary of a GB-scale log; advisors reason over that.
 *
 * <p>Implementations:
 * <ul>
 *   <li>{@code RuleBasedAdvisor} — deterministic, offline, always available (default).</li>
 *   <li>{@code LlmAdvisor} — sends the structured summary to an LLM for narrative root-cause
 *       reasoning and combined recommendations.</li>
 * </ul>
 * Both return the same {@link AnalysisResult.AiAdvice} shape so the report renders them
 * identically and they are interchangeable.
 */
public interface TuningAdvisor {

    /** A short identifier, e.g. "rule-based" or "llm:claude". */
    String name();

    /**
     * Produce advice for the given analysis. Implementations must be robust: on failure they
     * should return a degraded {@link AnalysisResult.AiAdvice} (or null) rather than throwing,
     * so report generation never breaks.
     */
    AnalysisResult.AiAdvice advise(AnalysisResult result);
}
