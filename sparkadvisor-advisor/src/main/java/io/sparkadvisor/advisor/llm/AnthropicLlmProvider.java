package io.sparkadvisor.advisor.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.sparkadvisor.core.util.Strings;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.nio.charset.StandardCharsets;

/**
 * {@link LlmProvider} for the Anthropic Messages API.
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
    private final CloseableHttpClient http;
    private final String apiKey;
    private final String model;
    private final String baseUrl;
    private final RequestConfig requestConfig;

    public AnthropicLlmProvider() {
        this(System.getenv("ANTHROPIC_API_KEY"), DEFAULT_MODEL, DEFAULT_BASE);
    }

    public AnthropicLlmProvider(String apiKey, String model, String baseUrl) {
        this.apiKey = apiKey;
        this.model = Strings.isBlank(model) ? DEFAULT_MODEL : model;
        this.baseUrl = Strings.isBlank(baseUrl) ? DEFAULT_BASE : baseUrl;
        this.requestConfig = RequestConfig.custom()
                .setConnectTimeout(15_000)
                .setConnectionRequestTimeout(15_000)
                .setSocketTimeout(60_000)
                .build();
        this.http = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build();
    }

    @Override
    public String name() {
        return "llm:claude";
    }

    @Override
    public String complete(String systemPrompt, String userPrompt) throws Exception {
        if (Strings.isBlank(apiKey)) {
            throw new IllegalStateException("No Anthropic API key (set ANTHROPIC_API_KEY)");
        }
        String body = buildRequestBody(systemPrompt, userPrompt);
        HttpPost req = new HttpPost(baseUrl);
        req.setConfig(requestConfig);
        req.setHeader("content-type", "application/json");
        req.setHeader("x-api-key", apiKey);
        req.setHeader("anthropic-version", API_VERSION);
        req.setEntity(new StringEntity(body,
                ContentType.APPLICATION_JSON.withCharset(StandardCharsets.UTF_8)));

        try (CloseableHttpResponse resp = http.execute(req)) {
            int status = resp.getStatusLine().getStatusCode();
            String respBody = resp.getEntity() == null
                    ? ""
                    : EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
            if (status / 100 != 2) {
                throw new RuntimeException("Anthropic API HTTP " + status + ": "
                        + truncate(respBody));
            }
            return extractText(respBody);
        }
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
