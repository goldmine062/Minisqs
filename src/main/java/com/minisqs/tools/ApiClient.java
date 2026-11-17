package com.minisqs.tools;

import com.minisqs.json.Json;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Small HTTP client for the broker, used by the producer, worker and test tools. */
public final class ApiClient {
    public static final class ApiException extends RuntimeException {
        public final int status;

        ApiException(int status, String message) {
            super(message);
            this.status = status;
        }
    }

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final String base;

    public ApiClient(String baseUrl) {
        this.base = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    public void createQueue(String name, double visibilitySeconds, int maxReceiveCount, String dlq) {
        String q = "name=" + enc(name) + "&visibilityTimeout=" + visibilitySeconds + "&maxReceiveCount=" + maxReceiveCount
                + (dlq == null ? "" : "&dlq=" + enc(dlq));
        call("POST", "/api/queues?" + q, null);
    }

    public String send(String queue, String body) {
        return (String) call("POST", "/api/queues/" + enc(queue) + "/messages", body).get("messageId");
    }

    @SuppressWarnings("unchecked")
    public List<String> sendBatch(String queue, List<String> bodies) {
        return (List<String>) call("POST", "/api/queues/" + enc(queue) + "/messages/batch", Json.write(bodies)).get("messageIds");
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> receive(String queue, int max, double waitSeconds) {
        Map<String, Object> r = call("POST", "/api/queues/" + enc(queue) + "/receive?max=" + max + "&wait=" + waitSeconds, null);
        return (List<Map<String, Object>>) r.get("messages");
    }

    /** Returns false if the receipt handle was no longer valid. */
    public boolean delete(String queue, String receiptHandle) {
        try {
            call("DELETE", "/api/queues/" + enc(queue) + "/messages?receiptHandle=" + enc(receiptHandle), null);
            return true;
        } catch (ApiException e) {
            if (e.status == 410) return false;
            throw e;
        }
    }

    /** Returns how many handles could not be deleted. */
    public int deleteBatch(String queue, List<String> receiptHandles) {
        List<?> failed = (List<?>) call("POST", "/api/queues/" + enc(queue) + "/delete-batch", Json.write(receiptHandles)).get("failed");
        return failed.size();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> stats(String queue) {
        return call("GET", "/api/queues/" + enc(queue), null);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> call(String method, String path, String body) {
        HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(30));
        req.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        try {
            HttpResponse<String> res = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
            Map<String, Object> json = (Map<String, Object>) Json.parse(res.body());
            if (res.statusCode() >= 400) {
                throw new ApiException(res.statusCode(), String.valueOf(json.get("message")));
            }
            return json;
        } catch (IOException e) {
            throw new ApiException(0, "can't reach broker at " + base + " (" + e.getClass().getSimpleName() + ")");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ApiException(0, "interrupted");
        }
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    static List<String> handles(List<Map<String, Object>> messages) {
        List<String> out = new ArrayList<>();
        for (Map<String, Object> m : messages) out.add((String) m.get("receiptHandle"));
        return out;
    }
}
