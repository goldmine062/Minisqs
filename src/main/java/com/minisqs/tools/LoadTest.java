package com.minisqs.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producers and consumers hammer one queue for a fixed time, then the test checks that every
 * acknowledged message was consumed (no loss) and counts duplicates (allowed by at-least-once).
 *
 * <pre>
 * java -jar minisqs.jar loadtest --producers 8 --consumers 8 --seconds 20 --batch 10 --body-bytes 256
 * </pre>
 */
public final class LoadTest {
    private LoadTest() {}

    /** Growable array of latencies, one per thread, so recording needs no locking. */
    private static final class Recorder {
        long[] values = new long[1 << 14];
        int size;

        void add(long v) {
            if (size == values.length) values = Arrays.copyOf(values, size * 2);
            values[size++] = v;
        }
    }

    public static void run(Args a) throws Exception {
        String url = a.str("url", "http://localhost:8080");
        String queue = a.str("queue", "loadtest");
        int producers = a.integer("producers", 8);
        int consumers = a.integer("consumers", 8);
        int seconds = a.integer("seconds", 20);
        int batch = Math.max(1, Math.min(10, a.integer("batch", 10)));
        int bodyBytes = a.integer("body-bytes", 256);

        ApiClient setup = new ApiClient(url);
        setup.createQueue(queue, 30, 0, null);
        String padding = "x".repeat(Math.max(0, bodyBytes - 40));

        AtomicBoolean producing = new AtomicBoolean(true);
        AtomicBoolean consuming = new AtomicBoolean(true);
        AtomicLong nextSeq = new AtomicLong();
        Set<Long> acked = ConcurrentHashMap.newKeySet();
        Set<Long> consumed = ConcurrentHashMap.newKeySet();
        AtomicLong duplicates = new AtomicLong();
        AtomicLong sendErrors = new AtomicLong();
        List<Recorder> sendLatency = new ArrayList<>();
        List<Recorder> endToEnd = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        System.out.printf("Load test: %d producers (batch %d), %d consumers, %d s, %d-byte messages against %s%n",
                producers, batch, consumers, seconds, bodyBytes, url);

        for (int p = 0; p < producers; p++) {
            Recorder rec = new Recorder();
            sendLatency.add(rec);
            threads.add(Thread.ofPlatform().start(() -> {
                ApiClient api = new ApiClient(url);
                while (producing.get()) {
                    List<String> bodies = new ArrayList<>(batch);
                    List<Long> seqs = new ArrayList<>(batch);
                    for (int i = 0; i < batch; i++) {
                        long seq = nextSeq.getAndIncrement();
                        seqs.add(seq);
                        bodies.add(seq + "|" + System.nanoTime() + "|" + padding);
                    }
                    long start = System.nanoTime();
                    try {
                        if (batch == 1) api.send(queue, bodies.get(0));
                        else api.sendBatch(queue, bodies);
                        rec.add(System.nanoTime() - start);
                        acked.addAll(seqs);
                    } catch (RuntimeException e) {
                        sendErrors.incrementAndGet();
                    }
                }
            }));
        }

        for (int c = 0; c < consumers; c++) {
            Recorder rec = new Recorder();
            endToEnd.add(rec);
            threads.add(Thread.ofPlatform().start(() -> {
                ApiClient api = new ApiClient(url);
                while (consuming.get()) {
                    List<Map<String, Object>> msgs;
                    try {
                        msgs = api.receive(queue, 10, 1);
                    } catch (RuntimeException e) {
                        continue;
                    }
                    if (msgs.isEmpty()) continue;
                    long now = System.nanoTime();
                    for (Map<String, Object> m : msgs) {
                        String[] parts = ((String) m.get("body")).split("\\|", 3);
                        long seq = Long.parseLong(parts[0]);
                        if (!consumed.add(seq)) duplicates.incrementAndGet();
                        rec.add(now - Long.parseLong(parts[1]));
                    }
                    try {
                        api.deleteBatch(queue, ApiClient.handles(msgs));
                    } catch (RuntimeException ignored) {
                        // message will be redelivered after the visibility timeout
                    }
                }
            }));
        }

        long startedAt = System.nanoTime();
        for (int s = 1; s <= seconds; s++) {
            Thread.sleep(1000);
            System.out.printf("  %2d s: sent %,d, consumed %,d%n", s, acked.size(), consumed.size());
        }
        producing.set(false);
        double producedSeconds = (System.nanoTime() - startedAt) / 1e9;
        long sentTotal;
        Thread.sleep(500); // let in-flight sends finish
        sentTotal = acked.size();

        long drainDeadline = System.nanoTime() + 30_000_000_000L;
        while (System.nanoTime() < drainDeadline && !consumed.containsAll(acked)) Thread.sleep(200);
        double totalSeconds = (System.nanoTime() - startedAt) / 1e9;
        consuming.set(false);
        for (Thread t : threads) t.join(5000);

        long missing = acked.stream().filter(seq -> !consumed.contains(seq)).count();
        System.out.println();
        System.out.printf("Sent (acknowledged):   %,d messages, %,.0f msg/s%n", sentTotal, sentTotal / producedSeconds);
        System.out.printf("Consumed:              %,d messages, %,.0f msg/s overall%n", consumed.size(), consumed.size() / totalSeconds);
        System.out.printf("Send latency per call: p50 %s, p99 %s, max %s%n",
                ms(percentile(sendLatency, 50)), ms(percentile(sendLatency, 99)), ms(percentile(sendLatency, 100)));
        System.out.printf("End-to-end latency:    p50 %s, p99 %s (send -> received by a consumer)%n",
                ms(percentile(endToEnd, 50)), ms(percentile(endToEnd, 99)));
        System.out.printf("Send errors:           %,d%n", sendErrors.get());
        System.out.printf("Duplicates:            %,d (allowed under at-least-once delivery)%n", duplicates.get());
        System.out.printf("Lost messages:         %,d %s%n", missing, missing == 0 ? "(every acknowledged message was delivered)" : "<-- PROBLEM");
    }

    private static long percentile(List<Recorder> recorders, double p) {
        int total = 0;
        for (Recorder r : recorders) total += r.size;
        if (total == 0) return 0;
        long[] all = new long[total];
        int i = 0;
        for (Recorder r : recorders) {
            System.arraycopy(r.values, 0, all, i, r.size);
            i += r.size;
        }
        Arrays.sort(all);
        int idx = (int) Math.min(total - 1, Math.max(0, Math.ceil(p / 100.0 * total) - 1));
        return all[idx];
    }

    private static String ms(long nanos) {
        return String.format("%.2f ms", nanos / 1e6);
    }
}
