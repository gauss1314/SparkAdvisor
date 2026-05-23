package io.sparkadvisor.core;

import io.sparkadvisor.core.eventlog.EventLogParser;
import io.sparkadvisor.core.eventlog.EventLogReader;
import io.sparkadvisor.core.model.ApplicationModel;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

/**
 * High-level entry point for {@code core}: given an HDFS path, produce an
 * {@link ApplicationModel}. Higher layers (analyzer/predictor/report) consume the model.
 */
public final class EventLogAnalyzer {

    private final Configuration hadoopConf;

    public EventLogAnalyzer() {
        this(new Configuration());
    }

    public EventLogAnalyzer(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    /**
     * Read and parse the event log at {@code path} (single file or rolling directory).
     */
    public ApplicationModel analyze(String path) throws IOException {
        try (EventLogReader reader = new EventLogReader(path, hadoopConf)) {
            boolean truncated = reader.maybeTruncated();
            List<EventLogParser.EventLogPart> parts = reader.open();
            EventLogParser parser = new EventLogParser();
            if (parts.size() == 1) {
                EventLogParser.EventLogPart p = parts.get(0);
                return parser.parse(p.stream(), p.sourceName(), truncated);
            }
            return parser.parseRolling(parts, truncated);
        }
    }
}
