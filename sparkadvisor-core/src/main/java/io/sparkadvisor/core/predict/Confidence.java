package io.sparkadvisor.core.predict;

/**
 * Confidence attached to every prediction. Predictions are cost-model ESTIMATES, not
 * guarantees, so the report must always surface this alongside the predicted value.
 */
public enum Confidence {
    LOW,
    MEDIUM,
    HIGH
}
