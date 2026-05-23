package io.sparkadvisor.ui;

import io.sparkadvisor.ui.render.EventLogPathResolver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the path-resolution logic, which is independent of Spark types (uses the String
 * constructor). The Spark-coupled parts of the plugin are verified on first compile against
 * Spark 3.5.1 (see // VERIFY@3.5.1 markers).
 */
class EventLogPathResolverTest {

    @Test
    void buildsBasicPath() {
        var r = new EventLogPathResolver("hdfs:///spark2x/eventLog");
        assertEquals("hdfs:///spark2x/eventLog/application_1_1", r.pathFor("application_1_1"));
    }

    @Test
    void stripsTrailingSlash() {
        var r = new EventLogPathResolver("hdfs:///spark2x/eventLog/");
        assertEquals("hdfs:///spark2x/eventLog/app_2", r.pathFor("app_2"));
    }

    @Test
    void handlesEmptyLogDir() {
        var r = new EventLogPathResolver("");
        assertEquals("app_3", r.pathFor("app_3"));
        assertFalse(r.isConfigured());
    }

    @Test
    void appendsAttemptId() {
        var r = new EventLogPathResolver("hdfs:///e");
        assertEquals("hdfs:///e/app_4_1", r.pathFor("app_4", "1"));
    }

    @Test
    void blankAttemptHasNoSuffix() {
        var r = new EventLogPathResolver("hdfs:///e");
        assertEquals("hdfs:///e/app_5", r.pathFor("app_5", ""));
        assertTrue(r.isConfigured());
    }
}
