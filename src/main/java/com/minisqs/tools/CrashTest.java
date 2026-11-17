package com.minisqs.tools;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Proves durability: every send the broker acknowledged must survive a hard kill.
 *
 * <pre>
 * 1. java -jar minisqs.jar crashtest-send --count 20000     (records every acknowledged message in acked.txt)
 * 2. kill the broker with kill -9 / End task while step 1 is still sending
 * 3. restart the broker
 * 4. java -jar minisqs.jar crashtest-verify                 (drains the queue and compares with acked.txt)
 * </pre>
 */
public final class CrashTest {
    private CrashTest() {}

    public static void send(Args a) throws Exception {
        String url = a.str("url", "http://localhost:8080");
        String queue = a.str("queue", "crashtest");
        int count = a.integer("count", 20000);
        int threads = a.integer("threads", 8);
        Path ackedFile = Path.of(a.str("acked-file", "acked.txt"));

        new ApiClient(url).createQueue(queue, 30, 0, null);
        AtomicInteger next = new AtomicInteger();
        AtomicInteger acked = new AtomicInteger();
        System.out.printf("Sending up to %,d messages with %d threads. Kill the broker while this runs.%n", count, threads);

        try (BufferedWriter out = Files.newBufferedWriter(ackedFile, StandardCharsets.UTF_8, CREATE, TRUNCATE_EXISTING, WRITE)) {
            List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                workers.add(Thread.ofPlatform().start(() -> {
                    ApiClient api = new ApiClient(url);
                    while (true) {
                        int i = next.getAndIncrement();
                        if (i >= count) return;
                        String body = "crash-" + i;
                        try {
                            api.send(queue, body);
                        } catch (RuntimeException e) {
                            return; // broker is gone: this send was never acknowledged
                        }
                        synchronized (out) {
                            try {
                                out.write(body);
                                out.newLine();
                                out.flush();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                        int n = acked.incrementAndGet();
                        if (n % 2000 == 0) System.out.printf("  %,d acknowledged%n", n);
                    }
                }));
            }
            for (Thread w : workers) w.join();
        }
        System.out.printf("Stopped. %,d sends were acknowledged by the broker (listed in %s).%n", acked.get(), ackedFile);
    }

    public static void verify(Args a) throws Exception {
        String url = a.str("url", "http://localhost:8080");
        String queue = a.str("queue", "crashtest");
        Path ackedFile = Path.of(a.str("acked-file", "acked.txt"));
        ApiClient api = new ApiClient(url);

        Set<String> expected = new HashSet<>(Files.readAllLines(ackedFile, StandardCharsets.UTF_8));
        expected.remove("");
        Set<String> found = new HashSet<>();
        int emptyPolls = 0;
        System.out.printf("Draining '%s' and checking %,d acknowledged messages...%n", queue, expected.size());
        while (emptyPolls < 2) {
            List<Map<String, Object>> msgs = api.receive(queue, 10, 1);
            if (msgs.isEmpty()) {
                emptyPolls++;
                continue;
            }
            emptyPolls = 0;
            for (Map<String, Object> m : msgs) found.add((String) m.get("body"));
            api.deleteBatch(queue, ApiClient.handles(msgs));
        }
        long lost = expected.stream().filter(b -> !found.contains(b)).count();
        long extra = found.stream().filter(b -> !expected.contains(b)).count();
        System.out.printf("Acknowledged before the crash: %,d%n", expected.size());
        System.out.printf("Found after restart:           %,d%n", found.size());
        System.out.printf("Lost:                          %,d %s%n", lost, lost == 0 ? "(PASS: no acknowledged message was lost)" : "(FAIL)");
        System.out.printf("Extra (sent but the ack never reached the client before the crash): %,d%n", extra);
    }
}
