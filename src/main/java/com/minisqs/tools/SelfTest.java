package com.minisqs.tools;

import com.minisqs.core.Broker;
import com.minisqs.core.QueueConfig;
import com.minisqs.core.QueueException;
import com.minisqs.core.QueueStats;
import com.minisqs.core.ReceivedMessage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests the broker directly (no HTTP). A fake clock makes timeout tests instant and deterministic.
 * Run: java -jar minisqs.jar selftest
 */
public final class SelfTest {
    private SelfTest() {}

    private static final AtomicLong clock = new AtomicLong(1_000_000);
    private static int passed;

    public static void run() throws Exception {
        test("send, receive, delete", SelfTest::basicFlow);
        test("visibility timeout hides a message, then it is redelivered", SelfTest::visibilityTimeout);
        test("stale receipt handle cannot delete after redelivery", SelfTest::staleReceiptHandle);
        test("message moves to the dead-letter queue after maxReceiveCount", SelfTest::deadLetterQueue);
        test("delayed message becomes visible later", SelfTest::delayedMessage);
        test("deduplication id returns the original message", SelfTest::deduplication);
        test("change visibility to 0 returns a message to the queue", SelfTest::releaseMessage);
        test("state survives a restart (replay of the log)", SelfTest::recovery);
        test("torn write at the end of the log is cut off on restart", SelfTest::tornWrite);
        test("compaction shrinks the log and keeps the state", SelfTest::compaction);
        test("long poll wakes up as soon as a message is sent", SelfTest::longPoll);
        test("concurrent producers and consumers: nothing lost", SelfTest::concurrency);
        System.out.printf("%nAll %d tests passed.%n", passed);
    }

    // ------------------------------------------------------------------ tests

    private static void basicFlow(Path dir) throws Exception {
        try (Broker b = open(dir)) {
            b.createQueue("q", cfg(30_000, 0, null));
            String id = b.send("q", "hello", null, null);
            List<ReceivedMessage> got = b.receive("q", 10, 0, null);
            check(got.size() == 1 && got.get(0).messageId().equals(id) && got.get(0).body().equals("hello"), "received the message");
            check(b.receive("q", 10, 0, null).isEmpty(), "in-flight message is not received twice");
            b.delete("q", got.get(0).receiptHandle());
            check(stats(b, "q").inFlight() == 0 && stats(b, "q").visible() == 0, "queue empty after delete");
        }
    }

    private static void visibilityTimeout(Path dir) throws Exception {
        try (Broker b = open(dir)) {
            b.createQueue("q", cfg(1_000, 0, null));
            b.send("q", "job", null, null);
            check(b.receive("q", 1, 0, null).size() == 1, "first receive");
            clock.addAndGet(999);
            check(b.receive("q", 1, 0, null).isEmpty(), "still hidden before the timeout");
            clock.addAndGet(2);
            List<ReceivedMessage> again = b.receive("q", 1, 0, null);
            check(again.size() == 1 && again.get(0).receiveCount() == 2, "redelivered with receiveCount 2");
        }
    }

    private static void staleReceiptHandle(Path dir) throws Exception {
        try (Broker b = open(dir)) {
            b.createQueue("q", cfg(1_000, 0, null));
            b.send("q", "job", null, null);
            String slowWorkerHandle = b.receive("q", 1, 0, null).get(0).receiptHandle();
            clock.addAndGet(1_001);
            String fastWorkerHandle = b.receive("q", 1, 0, null).get(0).receiptHandle();
            expectError(() -> b.delete("q", slowWorkerHandle), 410, "old handle rejected");
            b.delete("q", fastWorkerHandle);
            check(stats(b, "q").inFlight() == 0, "new handle deletes it");
        }
    }

    private static void deadLetterQueue(Path dir) throws Exception {
        try (Broker b = open(dir)) {
            b.createQueue("dlq", cfg(30_000, 0, null));
            b.createQueue("q", cfg(1_000, 2, "dlq"));
            b.send("q", "poison", null, null);
            for (int i = 0; i < 2; i++) {
                check(b.receive("q", 1, 0, null).size() == 1, "receive " + (i + 1));
                clock.addAndGet(1_001);
            }
            check(b.receive("q", 1, 0, null).isEmpty(), "third receive moves it instead of delivering");
            List<ReceivedMessage> dead = b.receive("dlq", 1, 0, null);
            check(dead.size() == 1 && dead.get(0).body().equals("poison"), "message is in the dead-letter queue");
            check(stats(b, "q").movedToDlq() == 1, "counter updated");
        }
    }

    private static void delayedMessage(Path dir) throws Exception {
        try (Broker b = open(dir)) {
            b.createQueue("q", cfg(30_000, 0, null));
            b.send("q", "later", 5_000L, null);
            check(b.receive("q", 1, 0, null).isEmpty() && stats(b, "q").delayed() == 1, "not visible yet");
            clock.addAndGet(5_000);
            check(b.receive("q", 1, 0, null).size() == 1, "visible after the delay");
        }
    }

    private static void deduplication(Path dir) throws Exception {
        try (Broker b = open(dir)) {
            b.createQueue("q", cfg(30_000, 0, null));
            String first = b.send("q", "pay order 7", null, "order-7");
            String retry = b.send("q", "pay order 7", null, "order-7");
            check(first.equals(retry), "same id returned for the retried send");
            check(stats(b, "q").visible() == 1, "only one message stored");
            clock.addAndGet(5 * 60 * 1000 + 1);
            check(!b.send("q", "pay order 7", null, "order-7").equals(first), "new message after the 5 minute window");
        }
    }

    private static void releaseMessage(Path dir) throws Exception {
        try (Broker b = open(dir)) {
            b.createQueue("q", cfg(30_000, 0, null));
            b.send("q", "job", null, null);
            String handle = b.receive("q", 1, 0, null).get(0).receiptHandle();
            b.changeVisibility("q", handle, 0);
            check(b.receive("q", 1, 0, null).size() == 1, "available again right away");
        }
    }

    private static void recovery(Path dir) throws Exception {
        String inFlightHandle;
        try (Broker b = open(dir)) {
            b.createQueue("dlq", cfg(30_000, 0, null));
            b.createQueue("q", cfg(10_000, 3, "dlq"));
            for (int i = 0; i < 5; i++) b.send("q", "m" + i, null, null);
            List<ReceivedMessage> two = b.receive("q", 2, 0, null);
            b.delete("q", two.get(0).receiptHandle());
            inFlightHandle = two.get(1).receiptHandle();
            b.send("q", "delayed", 60_000L, null);
        }
        try (Broker b = open(dir)) {
            QueueStats s = stats(b, "q");
            check(s.visible() == 3 && s.inFlight() == 1 && s.delayed() == 1,
                    "3 visible, 1 in flight, 1 delayed after restart (got " + s.visible() + "/" + s.inFlight() + "/" + s.delayed() + ")");
            b.delete("q", inFlightHandle);
            check(stats(b, "q").inFlight() == 0, "receipt handle from before the restart still works");
            check(b.listQueues().size() == 2, "both queues restored");
        }
    }

    private static void tornWrite(Path dir) throws Exception {
        try (Broker b = open(dir)) {
            b.createQueue("q", cfg(30_000, 0, null));
            for (int i = 0; i < 3; i++) b.send("q", "m" + i, null, null);
        }
        // Simulate a crash in the middle of writing a record: a length header promising 500 bytes, then 7 bytes.
        Files.write(dir.resolve("queue.wal"), new byte[]{0, 0, 1, (byte) 0xF4, 1, 2, 3, 4, 5, 6, 7}, StandardOpenOption.APPEND);
        try (Broker b = open(dir)) {
            check(b.recoveryInfo().truncatedBytes() == 11, "11 torn bytes cut off");
            check(stats(b, "q").visible() == 3, "earlier messages intact");
            b.send("q", "after", null, null);
        }
        try (Broker b = open(dir)) {
            check(stats(b, "q").visible() == 4, "log is usable after the repair");
        }
    }

    private static void compaction(Path dir) throws Exception {
        try (Broker b = open(dir)) {
            b.createQueue("q", cfg(30_000, 0, null));
            for (int i = 0; i < 2000; i++) {
                b.send("q", "message number " + i, null, null);
                if (i % 10 != 0) b.delete("q", b.receive("q", 1, 0, null).get(0).receiptHandle());
            }
            long visible = stats(b, "q").visible();
            Broker.CompactionResult r = b.compact();
            check(r.bytesAfter() < r.bytesBefore() / 5, "log shrank from " + r.bytesBefore() + " to " + r.bytesAfter() + " bytes");
            check(stats(b, "q").visible() == visible, "same state after compaction");
            b.send("q", "after compaction", null, null);
        }
        try (Broker b = open(dir)) {
            check(stats(b, "q").visible() + stats(b, "q").inFlight() == 201, "restart after compaction restores everything");
        }
    }

    private static void longPoll(Path dir) throws Exception {
        try (Broker b = Broker.open(dir, true)) { // real clock: this test really waits
            b.createQueue("q", cfg(30_000, 0, null));
            Thread sender = Thread.ofPlatform().start(() -> {
                try {
                    Thread.sleep(300);
                    b.send("q", "wake up", null, null);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            long start = System.nanoTime();
            List<ReceivedMessage> got = b.receive("q", 1, 10_000, null);
            long ms = (System.nanoTime() - start) / 1_000_000;
            sender.join();
            check(got.size() == 1 && ms >= 250 && ms < 2_000, "returned after " + ms + " ms, not after the 10 s wait");
        }
    }

    private static void concurrency(Path dir) throws Exception {
        try (Broker b = Broker.open(dir, true)) {
            b.createQueue("q", cfg(30_000, 0, null));
            int producers = 8, perProducer = 1000;
            Set<String> consumed = ConcurrentHashMap.newKeySet();
            AtomicInteger duplicates = new AtomicInteger();
            List<Thread> threads = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                int id = p;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        for (int i = 0; i < perProducer; i++) b.send("q", id + "-" + i, null, null);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            List<Thread> consumers = new ArrayList<>();
            for (int c = 0; c < 8; c++) {
                consumers.add(Thread.ofPlatform().start(() -> {
                    try {
                        while (consumed.size() < producers * perProducer) {
                            for (ReceivedMessage m : b.receive("q", 10, 200, null)) {
                                if (!consumed.add(m.body())) duplicates.incrementAndGet();
                                b.delete("q", m.receiptHandle());
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            for (Thread t : threads) t.join();
            for (Thread t : consumers) t.join(30_000);
            check(consumed.size() == producers * perProducer, "all " + producers * perProducer + " messages consumed");
            check(duplicates.get() == 0, "no message delivered twice while it was in flight");
            check(stats(b, "q").visible() == 0 && stats(b, "q").inFlight() == 0, "queue empty");
        }
    }

    // ------------------------------------------------------------------ helpers

    private interface TestBody {
        void run(Path dir) throws Exception;
    }

    private interface Action {
        void run() throws Exception;
    }

    private static void test(String name, TestBody body) throws Exception {
        Path dir = Files.createTempDirectory("minisqs-test");
        try {
            body.run(dir);
            passed++;
            System.out.println("PASS  " + name);
        } catch (AssertionError e) {
            System.out.println("FAIL  " + name + ": " + e.getMessage());
            throw e;
        } finally {
            try (var files = Files.walk(dir)) {
                files.sorted((x, y) -> y.compareTo(x)).forEach(p -> p.toFile().delete());
            }
        }
    }

    private static Broker open(Path dir) throws Exception {
        return Broker.open(dir, true, clock::get);
    }

    private static QueueConfig cfg(long visibilityMs, int maxReceives, String dlq) {
        return new QueueConfig(visibilityMs, maxReceives, dlq, 0);
    }

    private static QueueStats stats(Broker b, String q) {
        return b.stats(q);
    }

    private static void check(boolean condition, String what) {
        if (!condition) throw new AssertionError(what);
    }

    private static void expectError(Action action, int status, String what) throws Exception {
        try {
            action.run();
        } catch (QueueException e) {
            check(e.status == status, what + " (expected HTTP " + status + ", got " + e.status + ")");
            return;
        }
        throw new AssertionError(what + " (no error was thrown)");
    }
}
