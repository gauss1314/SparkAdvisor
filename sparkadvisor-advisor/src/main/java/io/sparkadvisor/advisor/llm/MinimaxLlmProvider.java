package io.sparkadvisor.advisor.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
 * {@link LlmProvider} for MiniMax text models, defaulting to MiniMax-M2.5.
 *
 * <p>Configuration via constructor or environment:
 * <ul>
 *   <li>API key: explicit, else {@code MINIMAX_API_KEY}.</li>
 *   <li>Model: explicit, else {@code MiniMax-M2.5}.</li>
 *   <li>Base URL: explicit, else MiniMax's OpenAI-compatible chat endpoint. Override with
 *       {@code MINIMAX_BASE_URL} for an internal gateway/proxy.</li>
 * </ul>
 *
 * <p>The request uses the OpenAI-compatible {@code /v1/chat/completions} shape so the provider
 * can be swapped behind gateways easily. The extractor also tolerates MiniMax legacy/direct
 * response fields to make cluster deployments less brittle.
 */
public final class MinimaxLlmProvider implements LlmProvider {

    private static final String DEFAULT_BASE = "https://api.minimax.io/v1/chat/completions";
    private static final String DEFAULT_MODEL = "MiniMax-M2.5";
    private static final int MAX_TOKENS = 1500;

    private final ObjectMapper mapper = new ObjectMapper();
    private final CloseableHttpClient http;
    private final String apiKey;
    private final String model;
    private final String baseUrl;
    private final RequestConfig requestConfig;

    public MinimaxLlmProvider() {
        this(System.getenv("MINIMAX_API_KEY"),
                envOrDefault("MINIMAX_MODEL", DEFAULT_MODEL),
                envOrDefault("MINIMAX_BASE_URL", DEFAULT_BASE));
    }

    public MinimaxLlmProvider(String apiKey, String model, String baseUrl) {
        this.apiKey = apiKey;
        this.model = (model == null || model.trim().isEmpty()) ? DEFAULT_MODEL : model;
        this.baseUrl = (baseUrl == null || baseUrl.trim().isEmpty()) ? DEFAULT_BASE : baseUrl;
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
        return "llm:minimax-m2.5";
    }

    @Override
    public String complete(String systemPrompt, String userPrompt) throws Exception {
        if (apiKey == null || apiKey.trim().isEmpty()) {
            throw new IllegalStateException("No MiniMax API key (set MINIMAX_API_KEY)");
        }
        String body = buildRequestBody(systemPrompt, userPrompt);
        HttpPost req = new HttpPost(baseUrl);
        req.setConfig(requestConfig);
        req.setHeader("content-type", "application/json");
        req.setHeader("authorization", "Bearer " + apiKey);
        req.setEntity(new StringEntity(body,
                ContentType.APPLICATION_JSON.withCharset(StandardCharsets.UTF_8)));

        try (CloseableHttpResponse resp = http.execute(req)) {
            int status = resp.getStatusLine().getStatusCode();
            String respBody = resp.getEntity() == null
                    ? ""
                    : EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
            if (status / 100 != 2) {
                throw new RuntimeException("MiniMax API HTTP " + status + ": "
                        + truncate(respBody));
            }
            return extractText(respBody);
        }
    }

    private String buildRequestBody(String system, String user) {
        ObjectNode root = mapper.createObjectNode();
        root.put("model", model);
        root.put("max_tokens", MAX_TOKENS);
        root.put("temperature", 0.2);
        ArrayNode messages = root.putArray("messages");
        ObjectNode systemMsg = messages.addObject();
        systemMsg.put("role", "system");
        systemMsg.put("content", system);
        ObjectNode userMsg = messages.addObject();
        userMsg.put("role", "user");
        userMsg.put("content", user);
        return root.toString();
    }

    private String extractText(String responseBody) throws Exception {
        JsonNode root = mapper.readTree(responseBody);

        JsonNode openAiContent = root.path("choices").path(0).path("message").path("content");
        if (!openAiContent.isMissingNode() && !openAiContent.asText("").trim().isEmpty()) {
            return openAiContent.asText();
        }

        JsonNode legacyReply = root.path("reply");
        if (!legacyReply.isMissingNode() && !legacyReply.asText("").trim().isEmpty()) {
            return legacyReply.asText();
        }

        JsonNode text = root.path("text");
        if (!text.isMissingNode() && !text.asText("").trim().isEmpty()) {
            return text.asText();
        }

        StringBuilder sb = new StringBuilder();
        JsonNode content = root.path("content");
        if (content.isArray()) {
            for (JsonNode block : content) {
                if ("text".equals(block.path("type").asText())) {
                    sb.append(block.path("text").asText());
                }
            }
        }
        return sb.toString();
    }

    private static String envOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.trim().isEmpty() ? fallback : value;
    }

    private static String truncate(String s) {
        if (s == null) return "";
        return s.length() <= 300 ? s : s.substring(0, 300) + "...";
    }
}
