package io.sparkadvisor.core.eventlog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Resolves a Spark event-log path on HDFS (or any Hadoop-compatible FS) into one or more
 * decompressed, newline-delimited JSON streams for {@link EventLogParser}.
 *
 * <h2>Path forms handled</h2>
 * <ul>
 *   <li><b>Single file</b>: {@code application_xxx} or {@code application_xxx.inprogress},
 *       possibly with a codec suffix ({@code .snappy}/{@code .lz4}/{@code .zstd}).</li>
 *   <li><b>Rolling directory</b>: {@code eventlog_v2_<appId>/} containing
 *       {@code events_<N>_<appId>} parts plus an {@code appstatus_<appId>} marker. Parts are
 *       returned in ascending index order; the {@code appstatus} marker is skipped.</li>
 * </ul>
 *
 * <p>Authentication: this class performs <b>no</b> Kerberos login. The process is expected to
 * already hold a valid TGT (the launcher script runs {@code kinit}); Hadoop's
 * {@code UserGroupInformation} picks up the ticket cache transparently.
 *
 * <p>Decompression is delegated to Hadoop's {@link CompressionCodecFactory} based on the file
 * suffix, so snappy/lz4/zstd/none all work as long as the codec is on the classpath.
 */
public final class EventLogReader implements AutoCloseable {

    private static final String ROLLING_DIR_PREFIX = "eventlog_v2_";
    private static final String EVENTS_PREFIX = "events_";
    private static final String APPSTATUS_PREFIX = "appstatus_";
    private static final String INPROGRESS_SUFFIX = ".inprogress";

    private final FileSystem fs;
    private final Path root;
    private final CompressionCodecFactory codecFactory;
    private final List<InputStream> opened = new ArrayList<>();

    public EventLogReader(String pathStr, Configuration conf) throws IOException {
        this.root = new Path(pathStr);
        this.fs = root.getFileSystem(conf);
        this.codecFactory = new CompressionCodecFactory(conf);
    }

    /** True if the log is (or appears) still being written. */
    public boolean maybeTruncated() throws IOException {
        if (fs.isDirectory(root)) {
            // Rolling dir is "in progress" until the appstatus marker indicates completion.
            // We treat presence of an ".inprogress" appstatus as truncated.
            for (FileStatus s : fs.listStatus(root)) {
                String n = s.getPath().getName();
                if (n.startsWith(APPSTATUS_PREFIX) && n.endsWith(INPROGRESS_SUFFIX)) {
                    return true;
                }
            }
            return false;
        }
        return root.getName().endsWith(INPROGRESS_SUFFIX);
    }

    /**
     * Open the log as an ordered list of decompressed parts.
     * Caller must {@link #close()} the reader to release streams.
     */
    public List<EventLogParser.EventLogPart> open() throws IOException {
        if (fs.isDirectory(root)) {
            return openRolling();
        }
        java.util.List<EventLogParser.EventLogPart> one = new java.util.ArrayList<EventLogParser.EventLogPart>();
        one.add(openOne(fs.getFileStatus(root)));
        return one;
    }

    private List<EventLogParser.EventLogPart> openRolling() throws IOException {
        FileStatus[] all = fs.listStatus(root);
        List<FileStatus> parts = new ArrayList<>();
        for (FileStatus s : all) {
            String name = s.getPath().getName();
            if (name.startsWith(EVENTS_PREFIX)) {
                parts.add(s);
            }
            // appstatus_* marker is intentionally ignored (no events).
        }
        // Order by the numeric index in events_<N>_<appId>.
        parts.sort(Comparator.comparingLong(s -> eventsIndex(s.getPath().getName())));
        List<EventLogParser.EventLogPart> result = new ArrayList<>();
        for (FileStatus s : parts) {
            result.add(openOne(s));
        }
        return result;
    }

    private EventLogParser.EventLogPart openOne(FileStatus status) throws IOException {
        Path p = status.getPath();
        InputStream raw = fs.open(p);
        CompressionCodec codec = codecFactory.getCodec(p);
        InputStream decoded = (codec == null) ? raw : codec.createInputStream(raw);
        opened.add(decoded);
        return new EventLogParser.EventLogPart(decoded, p.toString());
    }

    private static long eventsIndex(String name) {
        // events_<N>_<appId>  -> N
        try {
            String afterPrefix = name.substring(EVENTS_PREFIX.length());
            int underscore = afterPrefix.indexOf('_');
            String n = (underscore < 0) ? afterPrefix : afterPrefix.substring(0, underscore);
            return Long.parseLong(n);
        } catch (RuntimeException ex) {
            return Long.MAX_VALUE; // unknown ordering -> push to end
        }
    }

    public Path rootPath() {
        return root;
    }

    @Override
    public void close() {
        for (InputStream in : opened) {
            try {
                in.close();
            } catch (IOException ignored) {
                // best-effort
            }
        }
        opened.clear();
    }
}
