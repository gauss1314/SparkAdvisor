package io.sparkadvisor.monitor.checkpoint;

import io.sparkadvisor.report.i18n.ReportLanguage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ReplayCheckpointTest {

    @TempDir
    Path dir;

    @Test
    void storesAndReadsRenderedHtmlBySnapshotAndLanguage() throws Exception {
        ReplayCheckpoint checkpoint = new ReplayCheckpoint(dir);
        EventLogSnapshot snapshot = snapshot("k1");

        checkpoint.writeHtml(snapshot, "<section>en</section>", "<section>zh</section>");

        assertEquals("<section>en</section>", checkpoint.readHtml(snapshot, ReportLanguage.EN));
        assertEquals("<section>zh</section>", checkpoint.readHtml(snapshot, ReportLanguage.ZH));
    }

    @Test
    void missesWhenSnapshotKeyChanges() throws Exception {
        ReplayCheckpoint checkpoint = new ReplayCheckpoint(dir);
        checkpoint.writeHtml(snapshot("k1"), "<section>en</section>", "<section>zh</section>");

        assertNull(checkpoint.readHtml(snapshot("k2"), ReportLanguage.EN));
    }

    @Test
    void treatsAnalysisParametersAsPartOfCheckpointKey() throws Exception {
        ReplayCheckpoint checkpoint = new ReplayCheckpoint(dir);
        EventLogSnapshot snapshot = snapshot("base");
        EventLogSnapshot top50 = snapshot.withKey(snapshot.key() + "|top=50|bucketMs=3600000");
        EventLogSnapshot top10 = snapshot.withKey(snapshot.key() + "|top=10|bucketMs=3600000");

        checkpoint.writeHtml(top50, "<section>top50</section>", null);

        assertEquals("<section>top50</section>", checkpoint.readHtml(top50, ReportLanguage.EN));
        assertNull(checkpoint.readHtml(top10, ReportLanguage.EN));
    }

    private static EventLogSnapshot snapshot(String key) {
        return new EventLogSnapshot("hdfs:///events", key, 12L, 34L,
                List.of(new EventLogSnapshot.Part("events_1_app", "hdfs:///events/events_1_app", 12L, 34L)));
    }
}
