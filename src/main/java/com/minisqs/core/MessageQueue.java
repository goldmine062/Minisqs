package com.minisqs.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * One queue.
 *
 * <p>Core idea: every message lives in a single set ordered by {@code visibleAt}. A new message has
 * visibleAt = now (or now + delay). Receiving it pushes visibleAt to now + visibilityTimeout. So
 * "ready", "delayed" and "in flight" are not separate structures, just different visibleAt values, and
 * an expired visibility timeout needs no background thread: the message simply sorts to the front
 * again once its time has passed. Receive = take from the front while visibleAt <= now.
 *
 * <p>Every change is written to the WAL while holding the queue lock, so the log order matches the
 * order in which changes were applied in memory, and replaying the log rebuilds the same state.
 */
public final class MessageQueue {
    static final long DEDUP_WINDOW_MS = 5 * 60 * 1000L; // same 5-minute window as SQS FIFO queues

    private static final Comparator<Message> BY_VISIBILITY =
            Comparator.<Message>comparingLong(m -> m.visibleAt).thenComparingLong(m -> m.seq);

    record SendResult(String messageId, boolean duplicate, long walPosition) {}

    record ReceiveBatch(List<ReceivedMessage> delivered, List<Message> movedToDlq, long now) {}

    private record DedupEntry(String messageId, long expiresAt) {}

    private final String name;
    private final QueueConfig config;
    private final WriteAheadLog wal;
    private final LongSupplier clock;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();

    private final Map<String, Message> byId = new HashMap<>();
    private final Map<String, Message> byReceipt = new HashMap<>();
    private final TreeSet<Message> schedule = new TreeSet<>(BY_VISIBILITY);
    private final LinkedHashMap<String, DedupEntry> dedup = new LinkedHashMap<>();
    private long nextSeq;
    private boolean closed;

    // counters since process start (not rebuilt from the log)
    private long sentCount;
    private long receivedCount;
    private long deletedCount;
    private long movedToDlqCount;

    MessageQueue(String name, QueueConfig config, WriteAheadLog wal, LongSupplier clock) {
        this.name = name;
        this.config = config;
        this.wal = wal;
        this.clock = clock;
    }

    public String name() {
        return name;
    }

    public QueueConfig config() {
        return config;
    }

    // ------------------------------------------------------------------ live operations

    SendResult send(String body, Long delayMs, String dedupId) throws IOException {
        lock.lock();
        try {
            ensureOpen();
            long now = clock.getAsLong();
            if (dedupId != null) {
                purgeExpiredDedup(now);
                DedupEntry existing = dedup.get(dedupId);
                if (existing != null) {
                    return new SendResult(existing.messageId(), true, -1);
                }
            }
            return new SendResult(sendLocked(body, delayMs, dedupId, now), false, wal.sizeBytes());
        } finally {
            lock.unlock();
        }
    }

    /** Sends several messages under one lock acquisition; the caller does a single fsync for all. */
    List<String> sendBatch(List<String> bodies) throws IOException {
        lock.lock();
        try {
            ensureOpen();
            long now = clock.getAsLong();
            List<String> ids = new ArrayList<>(bodies.size());
            for (String body : bodies) {
                ids.add(sendLocked(body, null, null, now));
            }
            return ids;
        } finally {
            lock.unlock();
        }
    }

    private String sendLocked(String body, Long delayMs, String dedupId, long now) throws IOException {
        String id = UUID.randomUUID().toString();
        long visibleAt = now + (delayMs != null ? delayMs : config.delayMs());
        wal.append(WriteAheadLog.SEND,
                new Codec.Out().str(name).str(id).str(body).lng(now).lng(visibleAt).str(dedupId).toBytes());
        applySend(id, body, now, visibleAt, dedupId);
        sentCount++;
        return id;
    }

    /**
     * Non-blocking receive of up to {@code max} visible messages. Messages that have already been
     * received maxReceiveCount times are moved out (returned in movedToDlq) instead of delivered.
     */
    ReceiveBatch tryReceive(int max, long visibilityTimeoutMs, boolean dlqAvailable) throws IOException {
        lock.lock();
        try {
            ensureOpen();
            long now = clock.getAsLong();
            List<ReceivedMessage> delivered = new ArrayList<>();
            List<Message> moved = new ArrayList<>();
            List<Message> reinsert = new ArrayList<>();
            try {
                while (delivered.size() < max && !schedule.isEmpty() && schedule.first().visibleAt <= now) {
                    Message m = schedule.pollFirst();
                    try {
                        if (dlqAvailable && config.maxReceiveCount() > 0
                                && m.receiveCount >= config.maxReceiveCount()) {
                            wal.append(WriteAheadLog.MOVE_TO_DLQ, new Codec.Out()
                                    .str(name).str(m.id).str(config.deadLetterQueue()).lng(now).toBytes());
                            byId.remove(m.id);
                            if (m.receiptHandle != null) byReceipt.remove(m.receiptHandle);
                            moved.add(m);
                            movedToDlqCount++;
                            continue;
                        }
                        String handle = newReceiptHandle();
                        long visibleAt = now + visibilityTimeoutMs;
                        int count = m.receiveCount + 1;
                        wal.append(WriteAheadLog.RECEIVE, new Codec.Out()
                                .str(name).str(m.id).str(handle).lng(visibleAt).i32(count).toBytes());
                        if (m.receiptHandle != null) byReceipt.remove(m.receiptHandle); // old handle is now stale
                        m.receiptHandle = handle;
                        m.visibleAt = visibleAt;
                        m.receiveCount = count;
                        byReceipt.put(handle, m);
                        reinsert.add(m); // re-added after the loop so a 0s timeout can't deliver it twice
                        delivered.add(new ReceivedMessage(m.id, handle, m.body, count, m.sentAt));
                        receivedCount++;
                    } catch (IOException e) {
                        schedule.add(m);
                        throw e;
                    }
                }
            } finally {
                schedule.addAll(reinsert);
            }
            return new ReceiveBatch(delivered, moved, now);
        } finally {
            lock.unlock();
        }
    }

    long delete(String receiptHandle) throws IOException {
        lock.lock();
        try {
            ensureOpen();
            Message m = byReceipt.get(receiptHandle);
            if (m == null) throw QueueException.invalidReceiptHandle();
            long pos = wal.append(WriteAheadLog.DELETE, new Codec.Out().str(name).str(m.id).toBytes());
            applyDelete(m.id);
            deletedCount++;
            return pos;
        } finally {
            lock.unlock();
        }
    }

    /** Deletes several messages; returns the receipt handles that were invalid. */
    List<String> deleteBatch(List<String> receiptHandles) throws IOException {
        lock.lock();
        try {
            ensureOpen();
            List<String> failed = new ArrayList<>();
            for (String handle : receiptHandles) {
                Message m = byReceipt.get(handle);
                if (m == null) {
                    failed.add(handle);
                    continue;
                }
                wal.append(WriteAheadLog.DELETE, new Codec.Out().str(name).str(m.id).toBytes());
                applyDelete(m.id);
                deletedCount++;
            }
            return failed;
        } finally {
            lock.unlock();
        }
    }

    /** Extends (or with 0, ends) the invisibility of a message the caller is still holding. */
    long changeVisibility(String receiptHandle, long timeoutMs) throws IOException {
        lock.lock();
        try {
            ensureOpen();
            Message m = byReceipt.get(receiptHandle);
            if (m == null) throw QueueException.invalidReceiptHandle();
            long visibleAt = clock.getAsLong() + timeoutMs;
            long pos = wal.append(WriteAheadLog.CHANGE_VISIBILITY,
                    new Codec.Out().str(name).str(m.id).lng(visibleAt).toBytes());
            applyChangeVisibility(m.id, visibleAt);
            return pos;
        } finally {
            lock.unlock();
        }
    }

    /** Receives a message moved here from a source queue's dead-letter handling. */
    void acceptMoved(Message source, long visibleAt) {
        lock.lock();
        try {
            applyAcceptMoved(source, visibleAt);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Long-poll support: waits until a message may be visible, the queue changes, or maxWaitMs passes.
     * Callers re-check with tryReceive afterwards; a spurious wakeup is harmless.
     */
    void awaitVisible(long maxWaitMs) throws InterruptedException {
        lock.lock();
        try {
            if (closed) return;
            long now = clock.getAsLong();
            long wait = maxWaitMs;
            if (!schedule.isEmpty()) {
                long untilNext = schedule.first().visibleAt - now;
                if (untilNext <= 0) return;
                wait = Math.min(wait, untilNext);
            }
            if (wait > 0) {
                changed.await(wait, TimeUnit.MILLISECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    void close() {
        lock.lock();
        try {
            closed = true;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    QueueStats stats() {
        lock.lock();
        try {
            long now = clock.getAsLong();
            long visible = 0, inFlight = 0, delayed = 0, oldestSentAt = Long.MAX_VALUE;
            for (Message m : schedule) {
                if (m.visibleAt <= now) {
                    visible++;
                    oldestSentAt = Math.min(oldestSentAt, m.sentAt);
                } else if (m.receiptHandle != null) {
                    inFlight++;
                } else {
                    delayed++;
                }
            }
            long oldestAge = visible == 0 ? 0 : Math.max(0, now - oldestSentAt);
            return new QueueStats(name, config, visible, inFlight, delayed, oldestAge,
                    sentCount, receivedCount, deletedCount, movedToDlqCount);
        } finally {
            lock.unlock();
        }
    }

    /** Writes the queue's current contents as snapshot records (used by log compaction). */
    void snapshot(List<WriteAheadLog.Entry> out) {
        lock.lock();
        try {
            for (Message m : schedule) {
                out.add(new WriteAheadLog.Entry(WriteAheadLog.SNAPSHOT_MESSAGE, new Codec.Out()
                        .str(name).str(m.id).str(m.body).lng(m.sentAt).lng(m.visibleAt)
                        .i32(m.receiveCount).str(m.receiptHandle).toBytes()));
            }
            purgeExpiredDedup(clock.getAsLong());
            for (Map.Entry<String, DedupEntry> e : dedup.entrySet()) {
                out.add(new WriteAheadLog.Entry(WriteAheadLog.SNAPSHOT_DEDUP, new Codec.Out()
                        .str(name).str(e.getKey()).str(e.getValue().messageId())
                        .lng(e.getValue().expiresAt()).toBytes()));
            }
        } finally {
            lock.unlock();
        }
    }

    int size() {
        lock.lock();
        try {
            return byId.size();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ state changes (live + replay)
    // These only change memory. Live operations log first, then call them; replay calls them directly.

    void applySend(String id, String body, long sentAt, long visibleAt, String dedupId) {
        Message m = new Message(id, body, sentAt, visibleAt, nextSeq++);
        byId.put(id, m);
        schedule.add(m);
        if (dedupId != null) {
            dedup.put(dedupId, new DedupEntry(id, sentAt + DEDUP_WINDOW_MS));
        }
        signalWaiters();
    }

    void applyReceive(String id, String handle, long visibleAt, int receiveCount) {
        Message m = byId.get(id);
        if (m == null) return;
        schedule.remove(m);
        if (m.receiptHandle != null) byReceipt.remove(m.receiptHandle);
        m.receiptHandle = handle;
        m.visibleAt = visibleAt;
        m.receiveCount = receiveCount;
        byReceipt.put(handle, m);
        schedule.add(m);
    }

    void applyDelete(String id) {
        Message m = byId.remove(id);
        if (m == null) return;
        schedule.remove(m);
        if (m.receiptHandle != null) byReceipt.remove(m.receiptHandle);
    }

    void applyChangeVisibility(String id, long visibleAt) {
        Message m = byId.get(id);
        if (m == null) return;
        schedule.remove(m);
        m.visibleAt = visibleAt;
        schedule.add(m);
        signalWaiters();
    }

    Message applyRemove(String id) {
        Message m = byId.remove(id);
        if (m == null) return null;
        schedule.remove(m);
        if (m.receiptHandle != null) byReceipt.remove(m.receiptHandle);
        return m;
    }

    void applyAcceptMoved(Message source, long visibleAt) {
        Message m = new Message(source.id, source.body, source.sentAt, visibleAt, nextSeq++);
        byId.put(m.id, m);
        schedule.add(m);
        signalWaiters();
    }

    void applySnapshotMessage(String id, String body, long sentAt, long visibleAt, int receiveCount, String handle) {
        Message m = new Message(id, body, sentAt, visibleAt, nextSeq++);
        m.receiveCount = receiveCount;
        m.receiptHandle = handle;
        byId.put(id, m);
        schedule.add(m);
        if (handle != null) byReceipt.put(handle, m);
    }

    void applyDedup(String dedupId, String messageId, long expiresAt) {
        dedup.put(dedupId, new DedupEntry(messageId, expiresAt));
    }

    // ------------------------------------------------------------------ helpers

    private void purgeExpiredDedup(long now) {
        // Entries are inserted in send order, so expired ones are at the head.
        Iterator<DedupEntry> it = dedup.values().iterator();
        while (it.hasNext()) {
            if (it.next().expiresAt() <= now) it.remove();
            else break;
        }
    }

    /** Wakes long-polling receivers. During startup replay no lock is held and nobody is waiting. */
    private void signalWaiters() {
        if (lock.isHeldByCurrentThread()) changed.signalAll();
    }

    private void ensureOpen() {
        if (closed) throw QueueException.queueNotFound(name);
    }

    private static String newReceiptHandle() {
        return UUID.randomUUID().toString().replace("-", "") + UUID.randomUUID().toString().replace("-", "");
    }
}
