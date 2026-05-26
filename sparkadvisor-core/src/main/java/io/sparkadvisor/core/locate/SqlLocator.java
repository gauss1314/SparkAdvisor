package io.sparkadvisor.core.locate;

import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.SqlExecution;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Resolves a user-supplied identifier to one or more {@link SqlExecution}s.
 *
 * <p>Resolution order:
 * <ol>
 *   <li>Match against {@link SqlExecution#statementId()} (case-sensitive, trimmed).</li>
 *   <li>If no statementId matched and the input is purely numeric, fall back to
 *       matching {@link SqlExecution#executionId()}.</li>
 * </ol>
 *
 * <p>One StatementID may map to several executions (re-submitted statements, or a
 * single statement that triggers multiple actions). Results are returned slowest-first.
 */
public final class SqlLocator {

    private final ApplicationModel app;

    public SqlLocator(ApplicationModel app) {
        this.app = Objects.requireNonNull(app, "app");
    }

    /** @return matches ordered by wall-clock duration descending; empty if none. */
    public List<SqlExecution> locate(String id) {
        if (id == null || id.trim().isEmpty()) {
            return new java.util.ArrayList<SqlExecution>();
        }
        String key = id.trim();

        List<SqlExecution> byStatement = app.sqlExecutions().stream()
                .filter(s -> key.equals(s.statementId()))
                .sorted(Comparator.comparingLong(SqlExecution::wallClockMs).reversed())
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));

        if (!byStatement.isEmpty()) {
            return byStatement;
        }

        if (isNumeric(key)) {
            long execId = Long.parseLong(key);
            return app.sqlExecutions().stream()
                    .filter(s -> s.executionId() == execId)
                    .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
        }
        return new java.util.ArrayList<SqlExecution>();
    }

    /** Convenience: the single slowest match, or empty. */
    public java.util.Optional<SqlExecution> locateSlowest(String id) {
        List<SqlExecution> matches = locate(id);
        return matches.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(matches.get(0));
    }

    private static boolean isNumeric(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) return false;
        }
        return !s.isEmpty();
    }
}
