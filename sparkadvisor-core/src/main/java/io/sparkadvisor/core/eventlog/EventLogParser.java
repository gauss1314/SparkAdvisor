package io.sparkadvisor.core.eventlog;

import io.sparkadvisor.core.model.ApplicationModel;

import org.apache.spark.scheduler.ReplayListenerBus;

import java.io.InputStream;
import java.util.logging.Logger;

/**
 * Parses a Spark event log by replaying it through Spark's own {@link ReplayListenerBus}
 * (the same machinery the History Server uses), feeding a {@link SparkEventCollector}.
 *
 * <p>This deliberately reuses {@code ReplayListenerBus} + {@code JsonProtocol} instead of
 * hand-parsing JSON, so we inherit Spark's cross-version event compatibility.
 *
 * <p>Streaming: the bus reads one JSON event per line from the {@link InputStream}; we never
 * load the whole log into memory.
 */
public final class EventLogParser {

    private static final Logger LOG = Logger.getLogger(EventLogParser.class.getName());

    /**
     * Replay a single event-log stream.
     *
     * @param in             stream of newline-delimited JSON events (already decompressed)
     * @param sourceName     identifier used by Spark in warning messages
     * @param maybeTruncated true if the log may be truncated (.inprogress / abnormal end)
     * @return the accumulated application model
     */
    public ApplicationModel parse(InputStream in, String sourceName, boolean maybeTruncated) {
        ReplayListenerBus bus = new ReplayListenerBus();
        SparkEventCollector collector = new SparkEventCollector();
        bus.addListener(collector);
        // VERIFY@3.5.1: replay(InputStream, String, boolean) signature.
        bus.replay(in, sourceName, maybeTruncated);
        ApplicationModel model = collector.build();
        if (model.incomplete()) {
            LOG.warning(() -> "Event log appears incomplete/truncated: " + sourceName);
        }
        return model;
    }

    /**
     * Replay multiple ordered streams (rolling event-log directory) into a single model.
     * The collector is shared so state accumulates across files.
     *
     * @param parts ordered streams; each entry is [stream, sourceName]
     */
    public ApplicationModel parseRolling(java.util.List<EventLogPart> parts, boolean maybeTruncated) {
        ReplayListenerBus bus = new ReplayListenerBus();
        SparkEventCollector collector = new SparkEventCollector();
        bus.addListener(collector);
        for (int i = 0; i < parts.size(); i++) {
            EventLogPart part = parts.get(i);
            boolean lastPart = (i == parts.size() - 1);
            // Only the last part may be truncated.
            bus.replay(part.stream(), part.sourceName(), maybeTruncated && lastPart);
        }
        return collector.build();
    }

    /** One ordered piece of a rolling event log. */
    public record EventLogPart(InputStream stream, String sourceName) {}
}
