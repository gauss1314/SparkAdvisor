package io.sparkadvisor.monitor.checkpoint;

import io.sparkadvisor.core.util.Java8Collections;
import io.sparkadvisor.core.util.ValueObjects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Stable event-log snapshot key for queue analysis.
 *
 * <p>The key includes every rolling part's name, length, and modification time, avoiding the
 * weaker "path + total length" cache key that can miss part replacement or compaction changes.
 */
public final class EventLogSnapshot {

    private final String path;
    private final String key;
    private final long totalBytes;
    private final long modifiedAt;
    private final List<Part> parts;

    public EventLogSnapshot(String path, String key, long totalBytes, long modifiedAt, List<Part> parts) {
        this.path = path;
        this.key = key;
        this.totalBytes = totalBytes;
        this.modifiedAt = modifiedAt;
        this.parts = Java8Collections.listCopy(parts);
    }

    public static EventLogSnapshot fromPath(String pathStr, Configuration conf) throws IOException {
        Path path = new Path(pathStr);
        FileSystem fs = path.getFileSystem(conf);
        List<Part> parts = new ArrayList<Part>();
        if (fs.isDirectory(path)) {
            List<FileStatus> statuses = new ArrayList<FileStatus>();
            collectFiles(fs, path, statuses);
            for (FileStatus status : statuses) {
                parts.add(new Part(status.getPath().getName(), status.getPath().toString(),
                        status.getLen(), status.getModificationTime()));
            }
        } else {
            FileStatus status = fs.getFileStatus(path);
            parts.add(new Part(status.getPath().getName(), status.getPath().toString(),
                    status.getLen(), status.getModificationTime()));
        }
        long totalBytes = 0L;
        long modifiedAt = 0L;
        StringBuilder key = new StringBuilder(pathStr);
        for (Part part : parts) {
            totalBytes += part.length();
            modifiedAt = Math.max(modifiedAt, part.modifiedAt());
            key.append('|').append(part.name()).append(':')
                    .append(part.length()).append(':').append(part.modifiedAt());
        }
        return new EventLogSnapshot(pathStr, key.toString(), totalBytes, modifiedAt, parts);
    }

    private static void collectFiles(FileSystem fs, Path root, List<FileStatus> out) throws IOException {
        FileStatus[] statuses = fs.listStatus(root);
        java.util.Arrays.sort(statuses, Comparator.comparing(s -> s.getPath().toString()));
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                collectFiles(fs, status.getPath(), out);
            } else {
                out.add(status);
            }
        }
    }

    public String path() { return path; }
    public String key() { return key; }
    public long totalBytes() { return totalBytes; }
    public long modifiedAt() { return modifiedAt; }
    public List<Part> parts() { return parts; }

    public EventLogSnapshot withKey(String key) {
        return new EventLogSnapshot(path, key, totalBytes, modifiedAt, parts);
    }

    @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
    @Override public int hashCode(){return ValueObjects.hashFields(this);}
    @Override public String toString(){return ValueObjects.toString(this);}

    public static final class Part {
        private final String name;
        private final String path;
        private final long length;
        private final long modifiedAt;

        public Part(String name, String path, long length, long modifiedAt) {
            this.name = name;
            this.path = path;
            this.length = length;
            this.modifiedAt = modifiedAt;
        }

        public String name() { return name; }
        public String path() { return path; }
        public long length() { return length; }
        public long modifiedAt() { return modifiedAt; }

        @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
        @Override public int hashCode(){return ValueObjects.hashFields(this);}
        @Override public String toString(){return ValueObjects.toString(this);}
    }
}
