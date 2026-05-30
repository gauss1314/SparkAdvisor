package io.sparkadvisor.core.eventlog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.spark.SparkConf;
import io.sparkadvisor.core.util.Java8Collections;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
 * <p>Decompression first delegates to Spark History Server's own event-log opener so rolling
 * event-log files whose codec is encoded in Spark's event-log filename convention are decoded
 * exactly as Spark would decode them. If that internal API is unavailable, we fall back to
 * Hadoop's {@link CompressionCodecFactory} based on the file suffix.
 */
public final class EventLogReader implements AutoCloseable {

    private static final String ROLLING_DIR_PREFIX = "eventlog_v2_";
    private static final String EVENTS_PREFIX = "events_";
    private static final String APPSTATUS_PREFIX = "appstatus_";
    private static final String INPROGRESS_SUFFIX = ".inprogress";
    private static final int JSON_PROBE_BYTES = 64 * 1024;
    private static final String[] SPARK_EVENT_LOG_CODECS = {"zstd", "lz4", "snappy", "lzf"};

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
        return Java8Collections.listOf(openOne(fs.getFileStatus(root)));
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
        InputStream decoded = openWithSparkEventLogReader(p);
        if (decoded == null) {
            InputStream raw = fs.open(p);
            CompressionCodec codec = codecFactory.getCodec(p);
            decoded = (codec == null) ? raw : codec.createInputStream(raw);
        }
        decoded = ensureJsonEventStream(p, decoded);
        opened.add(decoded);
        return new EventLogParser.EventLogPart(decoded, p.toString());
    }

    private InputStream openWithSparkEventLogReader(Path p) throws IOException {
        try {
            // VERIFY@3.5.1: EventLogFileReader.openEventLog(Path, FileSystem) mirrors the
            // History Server path and handles Spark's event-log compression naming convention.
            Class<?> readerClass = Class.forName("org.apache.spark.deploy.history.EventLogFileReader");
            Method staticForwarder = readerClass.getMethod("openEventLog", Path.class, FileSystem.class);
            Object stream = staticForwarder.invoke(null, p, fs);
            return (InputStream) stream;
        } catch (ClassNotFoundException e) {
            return openWithSparkEventLogReaderCompanion(p);
        } catch (NoSuchMethodException e) {
            return openWithSparkEventLogReaderCompanion(p);
        } catch (IllegalAccessException e) {
            return openWithSparkEventLogReaderCompanion(p);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Spark EventLogFileReader.openEventLog failed for " + p, cause);
        }
    }

    private InputStream openWithSparkEventLogReaderCompanion(Path p) throws IOException {
        try {
            Class<?> companionClass = Class.forName("org.apache.spark.deploy.history.EventLogFileReader$");
            Object module = companionClass.getField("MODULE$").get(null);
            Method method = companionClass.getMethod("openEventLog", Path.class, FileSystem.class);
            Object stream = method.invoke(module, p, fs);
            return (InputStream) stream;
        } catch (ClassNotFoundException e) {
            return null;
        } catch (NoSuchMethodException e) {
            return null;
        } catch (NoSuchFieldException e) {
            return null;
        } catch (IllegalAccessException e) {
            return null;
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Spark EventLogFileReader.openEventLog failed for " + p, cause);
        }
    }

    private InputStream ensureJsonEventStream(Path p, InputStream in) throws IOException {
        BufferedInputStream buffered = buffered(in);
        if (looksLikeJsonLines(buffered)) {
            return buffered;
        }
        closeQuietly(buffered);

        for (int i = 0; i < SPARK_EVENT_LOG_CODECS.length; i++) {
            String codec = SPARK_EVENT_LOG_CODECS[i];
            InputStream candidate = null;
            try {
                candidate = openWithSparkCompressionCodec(p, codec);
                BufferedInputStream decoded = buffered(candidate);
                candidate = null;
                if (looksLikeJsonLines(decoded)) {
                    return decoded;
                }
                closeQuietly(decoded);
            } catch (ReflectiveOperationException ex) {
                closeQuietly(candidate);
            } catch (RuntimeException ex) {
                closeQuietly(candidate);
            }
        }

        throw new IOException("Event log stream is not newline-delimited JSON after Spark/Hadoop "
                + "decompression attempts: " + p + ". The file may use an unsupported Spark "
                + "event-log compression codec or be corrupt.");
    }

    private InputStream openWithSparkCompressionCodec(Path p, String codecName)
            throws IOException, ReflectiveOperationException {
        InputStream raw = fs.open(p);
        try {
            // VERIFY@3.5.1: Spark CompressionCodec.createCodec(SparkConf, String) and
            // compressedContinuousInputStream(InputStream) are what EventLogFileReader uses.
            Class<?> codecClass = Class.forName("org.apache.spark.io.CompressionCodec");
            Object codec = createSparkCompressionCodec(codecClass, codecName);
            Method method = codec.getClass().getMethod("compressedContinuousInputStream", InputStream.class);
            method.setAccessible(true);
            return (InputStream) method.invoke(codec, raw);
        } catch (InvocationTargetException ex) {
            closeQuietly(raw);
            Throwable cause = ex.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw ex;
        } catch (ReflectiveOperationException ex) {
            closeQuietly(raw);
            throw ex;
        } catch (RuntimeException ex) {
            closeQuietly(raw);
            throw ex;
        }
    }

    private static Object createSparkCompressionCodec(Class<?> codecClass, String codecName)
            throws ReflectiveOperationException {
        try {
            Method staticForwarder = codecClass.getMethod("createCodec", SparkConf.class, String.class);
            return staticForwarder.invoke(null, new SparkConf(false), codecName);
        } catch (NoSuchMethodException noStaticForwarder) {
            Class<?> companionClass = Class.forName("org.apache.spark.io.CompressionCodec$");
            Object module = companionClass.getField("MODULE$").get(null);
            Method method = companionClass.getMethod("createCodec", SparkConf.class, String.class);
            return method.invoke(module, new SparkConf(false), codecName);
        }
    }

    private static BufferedInputStream buffered(InputStream in) {
        if (in instanceof BufferedInputStream && in.markSupported()) {
            return (BufferedInputStream) in;
        }
        return new BufferedInputStream(in, JSON_PROBE_BYTES);
    }

    private static boolean looksLikeJsonLines(BufferedInputStream in) throws IOException {
        in.mark(JSON_PROBE_BYTES);
        int b;
        do {
            b = in.read();
        } while (b == 0xEF || b == 0xBB || b == 0xBF || b == ' ' || b == '\n' || b == '\r' || b == '\t');
        in.reset();
        return b == '{';
    }

    private static void closeQuietly(InputStream in) {
        if (in != null) {
            try {
                in.close();
            } catch (IOException ignored) {
                // best-effort
            }
        }
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
