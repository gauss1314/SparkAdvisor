package io.sparkadvisor.report.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.sparkadvisor.report.model.AnalysisResult;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Serializes {@link AnalysisResult} to JSON. This JSON IS the contract: the UI and the
 * future LLM advisor consume exactly this. Jackson 2.15 serializes Java records natively.
 */
public final class JsonReportWriter {

    private final ObjectMapper mapper;

    public JsonReportWriter() {
        this.mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    }

    public String toJson(AnalysisResult result) throws IOException {
        return mapper.writeValueAsString(result);
    }

    public void write(AnalysisResult result, Path out) throws IOException {
        Files.writeString(out, toJson(result));
    }

    public void write(AnalysisResult result, OutputStream out) throws IOException {
        mapper.writeValue(out, result);
    }
}
