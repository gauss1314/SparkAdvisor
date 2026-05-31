package io.sparkadvisor.monitor;

import io.sparkadvisor.core.util.Strings;
import io.sparkadvisor.core.util.ValueObjects;

/**
 * Metadata supplied by the caller around how a queue report was produced.
 *
 * <p>The event-log replay result remains the source of metrics; this context only controls
 * report metadata such as the stable snapshot key and whether SHS used an incremental path.
 */
public final class QueueAnalysisContext {

    private static final QueueAnalysisContext DEFAULT =
            new QueueAnalysisContext(null, false, "");

    private final String snapshotKey;
    private final boolean incremental;
    private final String degradedReason;

    public QueueAnalysisContext(String snapshotKey, boolean incremental, String degradedReason) {
        this.snapshotKey = snapshotKey;
        this.incremental = incremental;
        this.degradedReason = degradedReason == null ? "" : degradedReason;
    }

    public static QueueAnalysisContext defaults() {
        return DEFAULT;
    }

    public static QueueAnalysisContext fullSnapshot(String snapshotKey, String degradedReason) {
        return new QueueAnalysisContext(snapshotKey, false, degradedReason);
    }

    public static QueueAnalysisContext incremental(String snapshotKey) {
        return new QueueAnalysisContext(snapshotKey, true, "");
    }

    public String snapshotKey() {
        return snapshotKey;
    }

    public boolean hasSnapshotKey() {
        return !Strings.isBlank(snapshotKey);
    }

    public boolean incremental() {
        return incremental;
    }

    public String degradedReason() {
        return degradedReason;
    }

    @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
    @Override public int hashCode(){return ValueObjects.hashFields(this);}
    @Override public String toString(){return ValueObjects.toString(this);}
}
