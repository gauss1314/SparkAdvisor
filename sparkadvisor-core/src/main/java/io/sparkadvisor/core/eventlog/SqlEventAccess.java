package io.sparkadvisor.core.eventlog;

import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Access layer for SQL-related events.
 *
 * <p>{@code SparkListenerSQLExecutionStart/End} live in {@code spark-sql} and are referenced
 * with real types (spark-sql is a provided dependency, always present when analyzing SQL).
 *
 * <p>{@code SparkListenerThriftServerOperationStart} lives in {@code hive-thriftserver}, which
 * may NOT be on the classpath. We therefore access its {@code statement()} accessor
 * reflectively and degrade gracefully when the class/method is absent.
 */
final class SqlEventAccess {

    private static final Logger LOG = Logger.getLogger(SqlEventAccess.class.getName());

    private SqlEventAccess() {}

    static SparkListenerSQLExecutionStart sqlExecutionStart(SparkListenerEvent e) {
        // VERIFY@3.5.1: accessors executionId(), description(), physicalPlanDescription(), time()
        return (SparkListenerSQLExecutionStart) e;
    }

    static SparkListenerSQLExecutionEnd sqlExecutionEnd(SparkListenerEvent e) {
        // VERIFY@3.5.1: accessors executionId(), time()
        return (SparkListenerSQLExecutionEnd) e;
    }

    /**
     * Reflectively read {@code statement()} from a Thrift operation-start event.
     * Returns empty if the accessor is not available.
     */
    static Optional<String> thriftStatement(SparkListenerEvent e) {
        try {
            Method m = e.getClass().getMethod("statement"); // VERIFY@3.5.1
            Object v = m.invoke(e);
            return v == null ? Optional.empty() : Optional.of(v.toString());
        } catch (ReflectiveOperationException ex) {
            LOG.fine(() -> "Thrift statement() not accessible: " + ex.getMessage());
            return Optional.empty();
        }
    }
}
