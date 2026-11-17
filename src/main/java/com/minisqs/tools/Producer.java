package com.minisqs.tools;

/**
 * Sends messages at a steady pace so you can watch them flow through the dashboard.
 * java -jar minisqs.jar produce --queue orders --count 20 --interval-ms 500
 */
public final class Producer {
    private Producer() {}

    public static void run(Args a) throws Exception {
        ApiClient api = new ApiClient(a.str("url", "http://localhost:8080"));
        String queue = a.str("queue", "orders");
        int count = a.integer("count", 20);
        long intervalMs = a.integer("interval-ms", 500);

        if (a.bool("create", true)) {
            ensureQueues(api, queue, a.dbl("visibility", 10), a.integer("max-receives", 3));
        }
        for (int i = 1; i <= count; i++) {
            String body = "{\"orderId\": " + i + ", \"item\": \"item-" + i + "\"}";
            String id = api.send(queue, body);
            System.out.printf("sent order %d  (message %s)%n", i, id.substring(0, 8));
            if (intervalMs > 0) Thread.sleep(intervalMs);
        }
        System.out.println("done, sent " + count + " messages to " + queue);
    }

    /** Creates the queue and its dead-letter queue if they don't exist yet. */
    static void ensureQueues(ApiClient api, String queue, double visibility, int maxReceives) {
        String dlq = queue + "-dlq";
        try {
            api.createQueue(dlq, 30, 0, null);
            api.createQueue(queue, visibility, maxReceives, maxReceives > 0 ? dlq : null);
        } catch (ApiClient.ApiException e) {
            if (e.status != 409) throw e; // 409 = already exists with other settings; just use it
            System.out.println("note: " + e.getMessage());
        }
    }
}
