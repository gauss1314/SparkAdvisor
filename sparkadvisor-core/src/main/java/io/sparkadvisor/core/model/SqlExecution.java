package io.sparkadvisor.core.model;

import java.util.List;

/**
 * One SQL execution, identified by its {@code executionId} and (when available)
 * the {@code statementId} parsed from the leading {@code /* StatementID *}{@code /}
 * comment of the SQL text.
 *
 * @param statementId       extracted StatementID, or null if the SQL had no leading comment
 * @param description       the SQL text as seen in SparkListenerSQLExecutionStart.description
 * @param physicalPlanText  physicalPlanDescription (raw plan tree text), kept for the report
 * @param jobIds            jobs that belong to this execution
 */
public record SqlExecution(
        long executionId,
        String statementId,
        String description,
        String physicalPlanText,
        long startTime,
        long endTime,
        boolean incomplete,
        List<Long> jobIds) {

    public long wallClockMs() {
        if (startTime <= 0 || endTime <= 0) return 0;
        return endTime - startTime;
    }
}
