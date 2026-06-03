package io.sparkadvisor.core.eventlog;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventLogReaderPathResolutionTest {

    @TempDir
    Path dir;

    @Test
    void resolvesRollingDirectorySiblingFromAppIdCandidate() throws Exception {
        String appId = "spark-412e7687815849da9cf38719e24197f1";
        Path historyDir = dir.resolve("sparkK8sJobHistory");
        Path rollingDir = historyDir.resolve("eventlog_v2_" + appId);
        Files.createDirectories(rollingDir);

        try (EventLogReader reader = new EventLogReader(historyDir.resolve(appId).toString(), localConf())) {
            assertEquals(hadoopPath(rollingDir), reader.rootPath().toString());
        }
    }

    @Test
    void keepsDirectPathWhenItExists() throws Exception {
        String appId = "application_1_1";
        Path historyDir = dir.resolve("sparkHistory");
        Path directLog = historyDir.resolve(appId);
        Files.createDirectories(historyDir);
        Files.writeString(directLog, "{}\n");
        Files.createDirectories(historyDir.resolve("eventlog_v2_" + appId));

        try (EventLogReader reader = new EventLogReader(directLog.toString(), localConf())) {
            assertEquals(hadoopPath(directLog), reader.rootPath().toString());
        }
    }

    @Test
    void resolvesInProgressSingleFileSibling() throws Exception {
        String appId = "application_2_1";
        Path historyDir = dir.resolve("sparkHistory");
        Path inProgress = historyDir.resolve(appId + ".inprogress");
        Files.createDirectories(historyDir);
        Files.writeString(inProgress, "{}\n");

        try (EventLogReader reader = new EventLogReader(historyDir.resolve(appId).toString(), localConf())) {
            assertEquals(hadoopPath(inProgress), reader.rootPath().toString());
        }
    }

    private static Configuration localConf() {
        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        return conf;
    }

    private static String hadoopPath(Path path) {
        return new org.apache.hadoop.fs.Path(path.toString()).toString();
    }
}
