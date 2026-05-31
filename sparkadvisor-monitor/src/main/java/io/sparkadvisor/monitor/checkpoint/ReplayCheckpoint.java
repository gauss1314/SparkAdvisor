package io.sparkadvisor.monitor.checkpoint;

import io.sparkadvisor.report.i18n.ReportLanguage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

/**
 * File-backed SHS checkpoint for rendered queue reports.
 *
 * <p>This is intentionally a result checkpoint, not a byte-offset replay engine. It lets the
 * History Server serve the same event-log snapshot without re-parsing after process restart.
 * Meta still reports {@code incremental=false} until core supports safe offset replay and
 * aggregate-state merge.
 */
public final class ReplayCheckpoint {

    private static final String VERSION = "1";
    private static final String DIR_ENV = "SPARKADVISOR_QUEUE_CHECKPOINT_DIR";

    private final Path rootDir;

    public ReplayCheckpoint() {
        this(defaultRootDir());
    }

    public ReplayCheckpoint(Path rootDir) {
        this.rootDir = rootDir;
    }

    public String readHtml(EventLogSnapshot snapshot, ReportLanguage language) throws IOException {
        if (snapshot == null) {
            return null;
        }
        Entry entry = readEntry(snapshot);
        if (entry == null || !snapshot.key().equals(entry.snapshotKey)) {
            return null;
        }
        Path html = htmlPath(snapshot, language);
        if (!Files.isRegularFile(html)) {
            return null;
        }
        return new String(Files.readAllBytes(html), StandardCharsets.UTF_8);
    }

    public void writeHtml(EventLogSnapshot snapshot, String htmlEn, String htmlZh) throws IOException {
        if (snapshot == null) {
            return;
        }
        Files.createDirectories(rootDir);
        if (htmlEn != null) {
            writeAtomic(htmlPath(snapshot, ReportLanguage.EN), htmlEn);
        }
        if (htmlZh != null) {
            writeAtomic(htmlPath(snapshot, ReportLanguage.ZH), htmlZh);
        }
        Properties p = new Properties();
        p.setProperty("version", VERSION);
        p.setProperty("snapshotKey", snapshot.key());
        p.setProperty("path", snapshot.path());
        p.setProperty("totalBytes", String.valueOf(snapshot.totalBytes()));
        p.setProperty("modifiedAt", String.valueOf(snapshot.modifiedAt()));
        p.setProperty("parts", String.valueOf(snapshot.parts().size()));
        writeAtomic(propertiesPath(snapshot), p);
    }

    Entry readEntry(EventLogSnapshot snapshot) throws IOException {
        Path props = propertiesPath(snapshot);
        if (!Files.isRegularFile(props)) {
            return null;
        }
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(props)) {
            p.load(in);
        }
        if (!VERSION.equals(p.getProperty("version"))) {
            return null;
        }
        return new Entry(
                p.getProperty("snapshotKey", ""),
                parseLong(p.getProperty("totalBytes"), 0L),
                parseLong(p.getProperty("modifiedAt"), 0L));
    }

    private Path htmlPath(EventLogSnapshot snapshot, ReportLanguage language) {
        String suffix = language != null && language.isChinese() ? "zh.html" : "en.html";
        return rootDir.resolve(hash(snapshot.key()) + "." + suffix);
    }

    private Path propertiesPath(EventLogSnapshot snapshot) {
        return rootDir.resolve(hash(snapshot.key()) + ".properties");
    }

    private static void writeAtomic(Path path, String content) throws IOException {
        Files.createDirectories(path.getParent());
        Path tmp = path.resolveSibling(path.getFileName().toString() + ".tmp");
        Files.write(tmp, content.getBytes(StandardCharsets.UTF_8));
        try {
            Files.move(tmp, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException atomicMoveUnavailable) {
            Files.move(tmp, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void writeAtomic(Path path, Properties properties) throws IOException {
        Files.createDirectories(path.getParent());
        Path tmp = path.resolveSibling(path.getFileName().toString() + ".tmp");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            properties.store(out, "SparkAdvisor queue checkpoint");
        }
        try {
            Files.move(tmp, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException atomicMoveUnavailable) {
            Files.move(tmp, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static Path defaultRootDir() {
        String configured = System.getenv(DIR_ENV);
        if (configured != null && configured.trim().length() > 0) {
            return Paths.get(configured.trim());
        }
        return Paths.get(System.getProperty("java.io.tmpdir"), "sparkadvisor-queue-checkpoints");
    }

    private static long parseLong(String value, long fallback) {
        try {
            return Long.parseLong(value);
        } catch (RuntimeException e) {
            return fallback;
        }
    }

    private static String hash(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] bytes = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                out.append(String.format("%02x", b & 0xff));
            }
            return out.toString();
        } catch (NoSuchAlgorithmException e) {
            return Integer.toHexString(value.hashCode());
        }
    }

    public static final class Entry {
        private final String snapshotKey;
        private final long totalBytes;
        private final long modifiedAt;

        Entry(String snapshotKey, long totalBytes, long modifiedAt) {
            this.snapshotKey = snapshotKey;
            this.totalBytes = totalBytes;
            this.modifiedAt = modifiedAt;
        }

        public String snapshotKey() { return snapshotKey; }
        public long totalBytes() { return totalBytes; }
        public long modifiedAt() { return modifiedAt; }
    }
}
