package com.minisqs.tools;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A consumer that long-polls a queue, "processes" each message and deletes it.
 *
 * <pre>
 * java -jar minisqs.jar worker --queue orders --name w1 --work-ms 1000
 *   --fail-rate 0.3     fail 30% of messages (no delete, so they come back after the visibility timeout)
 *   --crash-after 3     exit abruptly after receiving 3 messages, without deleting the last one
 * </pre>
 */
public final class Worker {
    private Worker() {}

    public static void run(Args a) throws Exception {
        ApiClient api = new ApiClient(a.str("url", "http://localhost:8080"));
        String queue = a.str("queue", "orders");
        String name = a.str("name", "worker-" + ProcessHandle.current().pid());
        long workMs = a.integer("work-ms", 1000);
        double failRate = a.dbl("fail-rate", 0);
        int crashAfter = a.integer("crash-after", -1);

        System.out.printf("[%s] polling '%s' (work %d ms, fail rate %.0f%%%s)%n", name, queue, workMs, failRate * 100,
                crashAfter > 0 ? ", will crash after " + crashAfter + " messages" : "");
        int received = 0;
        while (true) {
            List<Map<String, Object>> batch;
            try {
                batch = api.receive(queue, 1, 20); // long poll: waits up to 20 s for a message
            } catch (ApiClient.ApiException e) {
                System.out.printf("[%s] %s, retrying in 2 s%n", name, e.getMessage());
                Thread.sleep(2000);
                continue;
            }
            for (Map<String, Object> m : batch) {
                received++;
                String body = (String) m.get("body");
                Object attempt = m.get("receiveCount");
                System.out.printf("[%s] got %s (attempt %s)%n", name, body, attempt);

                if (crashAfter > 0 && received >= crashAfter) {
                    System.out.printf("[%s] CRASH: exiting without deleting. The message stays invisible until its "
                            + "visibility timeout ends, then another worker gets it.%n", name);
                    Runtime.getRuntime().halt(1);
                }

                Thread.sleep(workMs);

                if (ThreadLocalRandom.current().nextDouble() < failRate) {
                    System.out.printf("[%s]   failed processing, not deleting; it will be retried%n", name);
                    continue;
                }
                if (api.delete(queue, (String) m.get("receiptHandle"))) {
                    System.out.printf("[%s]   done, deleted%n", name);
                } else {
                    System.out.printf("[%s]   too slow: visibility timeout passed and another worker took it; "
                            + "delete rejected (this is why processing must be idempotent)%n", name);
                }
            }
        }
    }
}
