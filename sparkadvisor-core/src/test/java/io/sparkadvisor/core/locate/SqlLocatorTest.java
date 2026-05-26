package io.sparkadvisor.core.locate;

import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.SqlExecution;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlLocatorTest {

    private static SqlExecution exec(long id, String stmtId, long durMs) {
        return new SqlExecution(id, stmtId, "/* " + stmtId + " */ select 1", "",
                1000L, 1000L + durMs, false, new java.util.ArrayList<>());
    }

    private static ApplicationModel appWith(SqlExecution... execs) {
        return new ApplicationModel("app-1", "test", 0, 0, false,
                new java.util.HashMap<>(), java.util.Arrays.asList(execs), new java.util.ArrayList<>(), new java.util.ArrayList<>(), new java.util.ArrayList<>(), new java.util.ArrayList<>());
    }

    @Test
    void locatesByStatementId() {
        var app = appWith(exec(1, "stmt_a", 100), exec(2, "stmt_b", 200));
        var loc = new SqlLocator(app);
        List<SqlExecution> r = loc.locate("stmt_b");
        assertEquals(1, r.size());
        assertEquals(2, r.get(0).executionId());
    }

    @Test
    void returnsMultipleMatchesSlowestFirst() {
        var app = appWith(
                exec(1, "dup", 100),
                exec(2, "dup", 500),
                exec(3, "dup", 250));
        var loc = new SqlLocator(app);
        List<SqlExecution> r = loc.locate("dup");
        assertEquals(3, r.size());
        assertEquals(2, r.get(0).executionId()); // 500ms first
        assertEquals(3, r.get(1).executionId()); // 250ms
        assertEquals(1, r.get(2).executionId()); // 100ms
    }

    @Test
    void fallsBackToNumericExecutionId() {
        var app = appWith(exec(7, "stmt_x", 100));
        var loc = new SqlLocator(app);
        // "7" doesn't match any statementId, so fall back to executionId.
        List<SqlExecution> r = loc.locate("7");
        assertEquals(1, r.size());
        assertEquals(7, r.get(0).executionId());
    }

    @Test
    void statementIdTakesPrecedenceOverNumericFallback() {
        // A statementId that happens to be numeric should match by statementId, not execId.
        var app = appWith(exec(99, "5", 100), exec(5, "other", 100));
        var loc = new SqlLocator(app);
        List<SqlExecution> r = loc.locate("5");
        assertEquals(1, r.size());
        assertEquals(99, r.get(0).executionId()); // matched by statementId "5"
    }

    @Test
    void emptyForUnknownId() {
        var app = appWith(exec(1, "stmt_a", 100));
        var loc = new SqlLocator(app);
        assertTrue(loc.locate("nope").isEmpty());
        assertTrue(loc.locate("  ").isEmpty());
        assertTrue(loc.locate(null).isEmpty());
    }

    @Test
    void locateSlowestReturnsTopMatch() {
        var app = appWith(exec(1, "dup", 100), exec(2, "dup", 900));
        var loc = new SqlLocator(app);
        assertEquals(2, loc.locateSlowest("dup").orElseThrow().executionId());
    }
}
