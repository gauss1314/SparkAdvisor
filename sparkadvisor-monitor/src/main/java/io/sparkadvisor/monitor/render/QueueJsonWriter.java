package io.sparkadvisor.monitor.render;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Serializes {@link QueueAnalysisResult}; this JSON is the queue-level contract.
 */
public final class QueueJsonWriter {

    private final ObjectMapper mapper =
            new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public String toJson(QueueAnalysisResult result) throws IOException {
        return mapper.writeValueAsString(result);
    }

    public void write(QueueAnalysisResult result, Path out) throws IOException {
        Files.writeString(out, toJson(result));
    }
}
