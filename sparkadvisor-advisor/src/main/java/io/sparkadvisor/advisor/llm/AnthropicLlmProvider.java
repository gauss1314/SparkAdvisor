package io.sparkadvisor.advisor.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * {@link LlmProvider} for the Anthropic Messages API, using only the JDK 21 built-in
 * {@link HttpClient} (no SDK dependency).
 *
 * <p>Configuration via constructor or environment:
 * <ul>
 *   <li>API key: explicit, else {@code ANTHROPIC_API_KEY}.</li>
 *   <li>Model: explicit, else a sensible default.</li>
 *   <li>Base URL: explicit, else the public endpoint (override for a gateway/proxy).</li>
 * </ul>
 *
 * <p>Note: in air-gapped clusters the endpoint may be unreachable; the {@code LlmAdvisor}
 * degrades gracefully on any failure. For on-prem, point {@code baseUrl} at an internal
 * gateway or use a local-model provider instead.
 */
public final class AnthropicLlmProvider implements LlmProvider {

    private static final String DEFAULT_BASE = "https://api.anthropic.com/v1/messages";
    private static final String DEFAULT_MODEL = "claude-sonnet-4-20250514";
    private static final String API_VERSION = "2023-06-01";
    private static final int MAX_TOKENS = 1500;

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http;
    private final String apiKey;
    private final String model;
    private final String baseUrl;

    public AnthropicLlmProvider() {
        this(System.getenv("ANTHROPIC_API_KEY"), DEFAULT_MODEL, DEFAULT_BASE);
    }

    public AnthropicLlmProvider(String apiKey, String model, String baseUrl) {
        this.apiKey = apiKey;
        this.model = (model == null || model.trim().isEmpty()) ? DEFAULT_MODEL : model;
        this.baseUrl = (baseUrl == null || baseUrl.trim().isEmpty()) ? DEFAULT_BASE : baseUrl;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(15))
                .build();
    }

    @Override
    public String name() {
        return "llm:claude";
    }

    @Override
    public String complete(String systemPrompt, String userPrompt) throws Exception {
        if (apiKey == null || apiKey.trim().isEmpty()) {
            throw new IllegalStateException("No Anthropic API key (set ANTHROPIC_API_KEY)");
        }
        String body = buildRequestBody(systemPrompt, userPrompt);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl))
                .timeout(Duration.ofSeconds(60))
                .header("content-type", "application/json")
                .header("x-api-key", apiKey)
                .header("anthropic-version", API_VERSION)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() / 100 != 2) {
            throw new RuntimeException("Anthropic API HTTP " + resp.statusCode() + ": "
                    + truncate(resp.body()));
        }
        return extractText(resp.body());
    }

    private String buildRequestBody(String system, String user) {
        ObjectNode root = mapper.createObjectNode();
        root.put("model", model);
        root.put("max_tokens", MAX_TOKENS);
        root.put("system", system);
        ArrayNode messages = root.putArray("messages");
        ObjectNode msg = messages.addObject();
        msg.put("role", "user");
        msg.put("content", user);
        return root.toString();
    }

    /** Extract concatenated text blocks from the Messages API response. */
    private String extractText(String responseBody) throws Exception {
        JsonNode root = mapper.readTree(responseBody);
        JsonNode content = root.path("content");
        StringBuilder sb = new StringBuilder();
        if (content.isArray()) {
            for (JsonNode block : content) {
                if ("text".equals(block.path("type").asText())) {
                    sb.append(block.path("text").asText());
                }
            }
        }
        return sb.toString();
    }

    private static String truncate(String s) {
        if (s == null) return "";
        return s.length() <= 300 ? s : s.substring(0, 300) + "...";
    }
}
