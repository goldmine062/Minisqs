package com.minisqs.server;

import com.minisqs.core.Broker;
import com.minisqs.core.QueueConfig;
import com.minisqs.core.QueueException;
import com.minisqs.core.QueueStats;
import com.minisqs.core.ReceivedMessage;
import com.minisqs.json.Json;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * REST API. Times are in seconds (decimals allowed), like SQS.
 *
 * <pre>
 * GET    /api/queues                                 list queues with stats
 * POST   /api/queues?name=&visibilityTimeout=&maxReceiveCount=&dlq=&delay=
 * GET    /api/queues/{q}                             stats for one queue
 * DELETE /api/queues/{q}
 * POST   /api/queues/{q}/messages?delay=&dedupId=    body = message text
 * POST   /api/queues/{q}/messages/batch              body = JSON array of strings
 * POST   /api/queues/{q}/receive?max=&wait=&visibilityTimeout=
 * DELETE /api/queues/{q}/messages?receiptHandle=
 * POST   /api/queues/{q}/delete-batch                body = JSON array of receipt handles
 * POST   /api/queues/{q}/visibility?receiptHandle=&timeout=
 * POST   /api/admin/compact
 * GET    /api/info                                   log size, fsyncs, recovery stats
 * GET    /metrics                                    Prometheus format
 * GET    /                                           dashboard
 * </pre>
 */
public final class HttpApi {
    private final Broker broker;
    private final HttpServer server;

    public HttpApi(Broker broker, int port) throws IOException {
        this.broker = broker;
        this.server = HttpServer.create(new InetSocketAddress(port), 1024);
        // One virtual thread per request: long polls that wait 20 s cost almost nothing.
        this.server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        this.server.createContext("/", this::handle);
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    private void handle(HttpExchange ex) {
        try {
            route(ex);
        } catch (QueueException e) {
            respondJson(ex, e.status, error(e.code, e.getMessage()));
        } catch (IllegalArgumentException e) {
            respondJson(ex, 400, error("InvalidParameter", e.getMessage()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            respondJson(ex, 503, error("ServiceUnavailable", "server is shutting down"));
        } catch (Exception e) {
            e.printStackTrace();
            respondJson(ex, 500, error("InternalError", String.valueOf(e.getMessage())));
        } finally {
            ex.close();
        }
    }

    private void route(HttpExchange ex) throws Exception {
        String method = ex.getRequestMethod();
        String path = ex.getRequestURI().getPath();
        Map<String, String> query = parseQuery(ex.getRequestURI().getRawQuery());

        if (path.equals("/") && method.equals("GET")) {
            respond(ex, 200, "text/html; charset=utf-8", Dashboard.HTML);
            return;
        }
        if (path.equals("/metrics") && method.equals("GET")) {
            respond(ex, 200, "text/plain; version=0.0.4", Metrics.render(broker));
            return;
        }

        List<String> seg = new ArrayList<>(Arrays.stream(path.split("/")).filter(s -> !s.isEmpty()).toList());
        if (seg.isEmpty() || !seg.get(0).equals("api")) {
            throw new QueueException(404, "NotFound", "no route for " + method + " " + path);
        }
        seg.remove(0);

        if (seg.equals(List.of("info")) && method.equals("GET")) {
            respondJson(ex, 200, info());
            return;
        }
        if (seg.equals(List.of("admin", "compact")) && method.equals("POST")) {
            Broker.CompactionResult r = broker.compact();
            respondJson(ex, 200, Map.of("bytesBefore", r.bytesBefore(), "bytesAfter", r.bytesAfter(),
                    "records", r.records()));
            return;
        }
        if (seg.isEmpty() || !seg.get(0).equals("queues")) {
            throw new QueueException(404, "NotFound", "no route for " + method + " " + path);
        }

        if (seg.size() == 1) {
            switch (method) {
                case "GET" -> {
                    List<Object> list = new ArrayList<>();
                    for (QueueStats s : broker.listQueues()) list.add(statsJson(s));
                    respondJson(ex, 200, Map.of("queues", list));
                }
                case "POST" -> {
                    QueueConfig cfg = new QueueConfig(
                            seconds(query, "visibilityTimeout", 30_000),
                            intParam(query, "maxReceiveCount", 0),
                            query.get("dlq"),
                            seconds(query, "delay", 0));
                    broker.createQueue(query.get("name"), cfg);
                    respondJson(ex, 200, statsJson(broker.stats(query.get("name"))));
                }
                default -> methodNotAllowed();
            }
            return;
        }

        String queue = seg.get(1);
        String action = seg.size() > 2 ? String.join("/", seg.subList(2, seg.size())) : "";
        switch (method + " " + action) {
            case "GET " -> respondJson(ex, 200, statsJson(broker.stats(queue)));
            case "DELETE " -> {
                broker.deleteQueue(queue);
                respondJson(ex, 200, Map.of("deleted", queue));
            }
            case "POST messages" -> {
                Long delay = query.containsKey("delay") ? seconds(query, "delay", 0) : null;
                String id = broker.send(queue, readBody(ex), delay, emptyToNull(query.get("dedupId")));
                respondJson(ex, 200, Map.of("messageId", id));
            }
            case "POST messages/batch" -> {
                List<String> ids = broker.sendBatch(queue, stringArray(readBody(ex)));
                respondJson(ex, 200, Map.of("messageIds", ids));
            }
            case "POST receive" -> {
                Long visibility = query.containsKey("visibilityTimeout")
                        ? seconds(query, "visibilityTimeout", 0) : null;
                List<ReceivedMessage> msgs = broker.receive(queue, intParam(query, "max", 1),
                        seconds(query, "wait", 0), visibility);
                List<Object> out = new ArrayList<>();
                for (ReceivedMessage m : msgs) {
                    Map<String, Object> j = new LinkedHashMap<>();
                    j.put("messageId", m.messageId());
                    j.put("receiptHandle", m.receiptHandle());
                    j.put("body", m.body());
                    j.put("receiveCount", m.receiveCount());
                    j.put("sentAt", m.sentAt());
                    out.add(j);
                }
                respondJson(ex, 200, Map.of("messages", out));
            }
            case "DELETE messages" -> {
                broker.delete(queue, query.get("receiptHandle"));
                respondJson(ex, 200, Map.of("deleted", true));
            }
            case "POST delete-batch" -> {
                List<String> failed = broker.deleteBatch(queue, stringArray(readBody(ex)));
                respondJson(ex, 200, Map.of("failed", failed));
            }
            case "POST visibility" -> {
                broker.changeVisibility(queue, query.get("receiptHandle"), seconds(query, "timeout", 0));
                respondJson(ex, 200, Map.of("updated", true));
            }
            default -> throw new QueueException(404, "NotFound", "no route for " + method + " " + path);
        }
    }

    // ------------------------------------------------------------------ JSON shapes

    private Map<String, Object> info() {
        Broker.RecoveryInfo r = broker.recoveryInfo();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("walBytes", broker.walSizeBytes());
        m.put("walAppends", broker.walAppends());
        m.put("walFsyncs", broker.walFsyncs());
        m.put("fsyncEnabled", broker.fsyncEnabled());
        m.put("recoveredRecords", r.recordsReplayed());
        m.put("recoveredMessages", r.messages());
        m.put("recoveredQueues", r.queues());
        m.put("recoveryMillis", r.millis());
        m.put("truncatedBytes", r.truncatedBytes());
        return m;
    }

    static Map<String, Object> statsJson(QueueStats s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", s.name());
        m.put("visibilityTimeout", s.config().visibilityTimeoutMs() / 1000.0);
        m.put("maxReceiveCount", s.config().maxReceiveCount());
        m.put("dlq", s.config().deadLetterQueue());
        m.put("delay", s.config().delayMs() / 1000.0);
        m.put("visible", s.visible());
        m.put("inFlight", s.inFlight());
        m.put("delayed", s.delayed());
        m.put("oldestMessageAgeMs", s.oldestVisibleAgeMs());
        m.put("sent", s.sent());
        m.put("received", s.received());
        m.put("deleted", s.deleted());
        m.put("movedToDlq", s.movedToDlq());
        return m;
    }

    private static Map<String, Object> error(String code, String message) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("error", code);
        m.put("message", message);
        return m;
    }

    // ------------------------------------------------------------------ request helpers

    private static String readBody(HttpExchange ex) throws IOException {
        try (InputStream in = ex.getRequestBody()) {
            byte[] bytes = in.readNBytes(Broker.MAX_BODY_BYTES * Broker.MAX_BATCH * 2 + 1);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private static List<String> stringArray(String json) {
        Object parsed = Json.parse(json);
        if (!(parsed instanceof List<?> list)) throw new IllegalArgumentException("body must be a JSON array of strings");
        List<String> out = new ArrayList<>();
        for (Object o : list) {
            if (!(o instanceof String s)) throw new IllegalArgumentException("body must be a JSON array of strings");
            out.add(s);
        }
        return out;
    }

    private static Map<String, String> parseQuery(String raw) {
        Map<String, String> out = new HashMap<>();
        if (raw == null || raw.isEmpty()) return out;
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            String k = eq < 0 ? pair : pair.substring(0, eq);
            String v = eq < 0 ? "" : pair.substring(eq + 1);
            out.put(URLDecoder.decode(k, StandardCharsets.UTF_8), URLDecoder.decode(v, StandardCharsets.UTF_8));
        }
        return out;
    }

    /** Reads a time parameter given in seconds and returns milliseconds. */
    private static long seconds(Map<String, String> q, String key, long defaultMs) {
        String v = q.get(key);
        if (v == null || v.isEmpty()) return defaultMs;
        try {
            return Math.round(Double.parseDouble(v) * 1000);
        } catch (NumberFormatException e) {
            throw QueueException.badRequest(key + " must be a number of seconds");
        }
    }

    private static int intParam(Map<String, String> q, String key, int def) {
        String v = q.get(key);
        if (v == null || v.isEmpty()) return def;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw QueueException.badRequest(key + " must be an integer");
        }
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }

    private static void methodNotAllowed() {
        throw new QueueException(405, "MethodNotAllowed", "method not allowed");
    }

    // ------------------------------------------------------------------ responses

    private static void respondJson(HttpExchange ex, int status, Object body) {
        respond(ex, status, "application/json", Json.write(body));
    }

    private static void respond(HttpExchange ex, int status, String contentType, String body) {
        try {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", contentType);
            ex.getResponseHeaders().set("Cache-Control", "no-store");
            ex.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = ex.getResponseBody()) {
                out.write(bytes);
            }
        } catch (IOException ignored) {
            // client went away
        }
    }
}
