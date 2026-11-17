package com.minisqs.core;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

/**
 * The message broker: owns all queues and the write-ahead log.
 *
 * <p>Durability rules (the trade-offs worth explaining in an interview):
 * <ul>
 *   <li>Create queue and send are fsynced before returning. An acknowledged send is never lost.</li>
 *   <li>Receive, delete, change-visibility and dead-letter moves are logged but not fsynced. If the
 *       machine crashes before the OS flushes them, the worst case is that a message is delivered
 *       again. That is allowed under at-least-once delivery and saves an fsync per call.</li>
 * </ul>
 *
 * <p>Concurrency: each queue has its own lock, so queues don't block each other. A broker-wide
 * read-write lock lets normal operations run in parallel (read side) while queue create/delete and
 * log compaction get exclusive access (write side).
 */
public final class Broker implements Closeable {
    public static final int MAX_BODY_BYTES = 256 * 1024;
    public static final int MAX_BATCH = 10;
    public static final long MAX_WAIT_MS = 20_000;
    private static final Pattern QUEUE_NAME = Pattern.compile("[A-Za-z0-9_-]{1,80}");
    private static final String WAL_FILE = "queue.wal";
    private static final String COMPACT_FILE = "queue.wal.compacting";

    public record RecoveryInfo(int recordsReplayed, long truncatedBytes, long millis, int queues, long messages) {}

    public record CompactionResult(long bytesBefore, long bytesAfter, int records) {}

    private final Path dataDir;
    private final WriteAheadLog wal;
    private final LongSupplier clock;
    private final ConcurrentHashMap<String, MessageQueue> queues = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final RecoveryInfo recovery;
    private volatile long sizeAfterLastCompaction;

    public static Broker open(Path dataDir, boolean fsync) throws IOException {
        return open(dataDir, fsync, System::currentTimeMillis);
    }

    public static Broker open(Path dataDir, boolean fsync, LongSupplier clock) throws IOException {
        Files.createDirectories(dataDir);
        Files.deleteIfExists(dataDir.resolve(COMPACT_FILE)); // leftover from a crash during compaction
        return new Broker(dataDir, new WriteAheadLog(dataDir.resolve(WAL_FILE), fsync), clock);
    }

    private Broker(Path dataDir, WriteAheadLog wal, LongSupplier clock) throws IOException {
        this.dataDir = dataDir;
        this.wal = wal;
        this.clock = clock;
        long start = System.nanoTime();
        int records = wal.replay(this::apply);
        long messages = queues.values().stream().mapToLong(MessageQueue::size).sum();
        this.recovery = new RecoveryInfo(records, wal.truncatedBytesOnRecovery(),
                (System.nanoTime() - start) / 1_000_000, queues.size(), messages);
        this.sizeAfterLastCompaction = wal.sizeBytes();
    }

    // ------------------------------------------------------------------ queues

    public void createQueue(String name, QueueConfig config) throws IOException {
        if (name == null || !QUEUE_NAME.matcher(name).matches()) {
            throw QueueException.badRequest("queue name must be 1-80 characters: letters, digits, '-' or '_'");
        }
        stateLock.writeLock().lock();
        try {
            MessageQueue existing = queues.get(name);
            if (existing != null) {
                if (existing.config().equals(config)) return; // idempotent create
                throw QueueException.conflict("queue '" + name + "' already exists with different settings");
            }
            String dlq = config.deadLetterQueue();
            if (dlq != null) {
                if (dlq.equals(name)) throw QueueException.badRequest("a queue can't be its own dead-letter queue");
                if (!queues.containsKey(dlq)) {
                    throw QueueException.badRequest("dead-letter queue '" + dlq + "' does not exist; create it first");
                }
            }
            long pos = wal.append(WriteAheadLog.CREATE_QUEUE, encodeCreate(name, config));
            queues.put(name, new MessageQueue(name, config, wal, clock));
            wal.sync(pos);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    public void deleteQueue(String name) throws IOException {
        stateLock.writeLock().lock();
        try {
            MessageQueue q = require(name);
            for (MessageQueue other : queues.values()) {
                if (name.equals(other.config().deadLetterQueue())) {
                    throw QueueException.conflict("queue '" + other.name() + "' uses it as its dead-letter queue");
                }
            }
            long pos = wal.append(WriteAheadLog.DELETE_QUEUE, new Codec.Out().str(name).toBytes());
            queues.remove(name);
            q.close();
            wal.sync(pos);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    public List<QueueStats> listQueues() {
        List<QueueStats> out = new ArrayList<>();
        for (MessageQueue q : queues.values()) out.add(q.stats());
        out.sort(Comparator.comparing(QueueStats::name));
        return out;
    }

    public QueueStats stats(String queue) {
        return require(queue).stats();
    }

    // ------------------------------------------------------------------ messages

    /** Returns the message id. Durable before it returns. */
    public String send(String queue, String body, Long delayMs, String dedupId) throws IOException {
        validateBody(body);
        if (delayMs != null && (delayMs < 0 || delayMs > QueueConfig.MAX_DELAY_MS)) {
            throw QueueException.badRequest("delay must be between 0 and 900 seconds");
        }
        stateLock.readLock().lock();
        try {
            MessageQueue.SendResult r = require(queue).send(body, delayMs, dedupId);
            wal.sync(r.walPosition());
            return r.messageId();
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public List<String> sendBatch(String queue, List<String> bodies) throws IOException {
        if (bodies.isEmpty() || bodies.size() > MAX_BATCH) {
            throw QueueException.badRequest("a batch must have 1-" + MAX_BATCH + " messages");
        }
        for (String body : bodies) validateBody(body);
        stateLock.readLock().lock();
        try {
            List<String> ids = require(queue).sendBatch(bodies);
            wal.sync(wal.sizeBytes()); // one fsync for the whole batch
            return ids;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * Receives up to {@code max} messages. With waitMs > 0 this is a long poll: if nothing is visible
     * it blocks until a message arrives or becomes visible, or until the wait runs out.
     */
    public List<ReceivedMessage> receive(String queue, int max, long waitMs, Long visibilityTimeoutMs)
            throws IOException, InterruptedException {
        if (max < 1 || max > MAX_BATCH) throw QueueException.badRequest("max must be 1-" + MAX_BATCH);
        if (waitMs < 0 || waitMs > MAX_WAIT_MS) throw QueueException.badRequest("wait must be 0-20 seconds");
        if (visibilityTimeoutMs != null
                && (visibilityTimeoutMs < 0 || visibilityTimeoutMs > QueueConfig.MAX_VISIBILITY_TIMEOUT_MS)) {
            throw QueueException.badRequest("visibilityTimeout must be between 0 and 43200 seconds");
        }
        long deadline = System.nanoTime() + waitMs * 1_000_000L;
        while (true) {
            MessageQueue q;
            MessageQueue.ReceiveBatch batch;
            stateLock.readLock().lock();
            try {
                q = require(queue);
                QueueConfig cfg = q.config();
                long timeout = visibilityTimeoutMs != null ? visibilityTimeoutMs : cfg.visibilityTimeoutMs();
                String dlqName = cfg.deadLetterQueue();
                MessageQueue dlq = dlqName == null ? null : queues.get(dlqName);
                batch = q.tryReceive(max, timeout, dlq != null);
                for (Message m : batch.movedToDlq()) {
                    dlq.acceptMoved(m, batch.now());
                }
            } finally {
                stateLock.readLock().unlock();
            }
            if (!batch.delivered().isEmpty()) return batch.delivered();
            if (!batch.movedToDlq().isEmpty()) continue; // skipped poison messages, look again
            long remainingMs = (deadline - System.nanoTime()) / 1_000_000L;
            if (remainingMs <= 0) return List.of();
            q.awaitVisible(remainingMs); // waits outside the broker lock so compaction is never blocked
        }
    }

    public void delete(String queue, String receiptHandle) throws IOException {
        if (receiptHandle == null || receiptHandle.isEmpty()) throw QueueException.badRequest("receiptHandle is required");
        withReadLock(() -> require(queue).delete(receiptHandle));
    }

    /** Returns the receipt handles that could not be deleted. */
    public List<String> deleteBatch(String queue, List<String> receiptHandles) throws IOException {
        if (receiptHandles.isEmpty() || receiptHandles.size() > MAX_BATCH) {
            throw QueueException.badRequest("a batch must have 1-" + MAX_BATCH + " receipt handles");
        }
        stateLock.readLock().lock();
        try {
            return require(queue).deleteBatch(receiptHandles);
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public void changeVisibility(String queue, String receiptHandle, long timeoutMs) throws IOException {
        if (timeoutMs < 0 || timeoutMs > QueueConfig.MAX_VISIBILITY_TIMEOUT_MS) {
            throw QueueException.badRequest("timeout must be between 0 and 43200 seconds");
        }
        withReadLock(() -> require(queue).changeVisibility(receiptHandle, timeoutMs));
    }

    // ------------------------------------------------------------------ log maintenance

    /**
     * Rewrites the log so it contains only the current state (one record per live message) instead of
     * the full history. Same idea as compaction in an LSM tree: drop overwritten and deleted entries.
     * Written to a temp file, fsynced, then atomically renamed over the old log, so a crash at any
     * point leaves either the old or the new log intact.
     */
    public CompactionResult compact() throws IOException {
        stateLock.writeLock().lock();
        try {
            long before = wal.sizeBytes();
            List<WriteAheadLog.Entry> entries = new ArrayList<>();
            // dead-letter queues first so they exist before queues that reference them
            List<MessageQueue> ordered = new ArrayList<>(queues.values());
            ordered.sort(Comparator.comparing((MessageQueue q) -> q.config().deadLetterQueue() != null));
            for (MessageQueue q : ordered) {
                entries.add(new WriteAheadLog.Entry(WriteAheadLog.CREATE_QUEUE, encodeCreate(q.name(), q.config())));
            }
            for (MessageQueue q : ordered) q.snapshot(entries);
            Path tmp = dataDir.resolve(COMPACT_FILE);
            WriteAheadLog.writeFile(tmp, entries);
            wal.replaceWith(tmp);
            sizeAfterLastCompaction = wal.sizeBytes();
            return new CompactionResult(before, sizeAfterLastCompaction, entries.size());
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /** Compacts when the log is above the threshold and has at least doubled since the last compaction. */
    public CompactionResult compactIfNeeded(long thresholdBytes) throws IOException {
        long size = wal.sizeBytes();
        if (size > thresholdBytes && size > 2 * sizeAfterLastCompaction) return compact();
        return null;
    }

    public RecoveryInfo recoveryInfo() {
        return recovery;
    }

    public long walSizeBytes() {
        return wal.sizeBytes();
    }

    public long walFsyncs() {
        return wal.fsyncCount();
    }

    public long walAppends() {
        return wal.appendCount();
    }

    public boolean fsyncEnabled() {
        return wal.fsyncEnabled();
    }

    @Override
    public void close() throws IOException {
        stateLock.writeLock().lock();
        try {
            for (MessageQueue q : queues.values()) q.close();
            wal.close();
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    // ------------------------------------------------------------------ replay

    private void apply(WriteAheadLog.Entry e) {
        Codec.In in = new Codec.In(e.payload());
        switch (e.type()) {
            case WriteAheadLog.CREATE_QUEUE -> {
                String name = in.str();
                QueueConfig cfg = new QueueConfig(in.lng(), in.i32(), in.str(), in.lng());
                queues.put(name, new MessageQueue(name, cfg, wal, clock));
            }
            case WriteAheadLog.DELETE_QUEUE -> {
                MessageQueue q = queues.remove(in.str());
                if (q != null) q.close();
            }
            case WriteAheadLog.SEND -> {
                MessageQueue q = queues.get(in.str());
                if (q != null) q.applySend(in.str(), in.str(), in.lng(), in.lng(), in.str());
            }
            case WriteAheadLog.RECEIVE -> {
                MessageQueue q = queues.get(in.str());
                if (q != null) q.applyReceive(in.str(), in.str(), in.lng(), in.i32());
            }
            case WriteAheadLog.DELETE -> {
                MessageQueue q = queues.get(in.str());
                if (q != null) q.applyDelete(in.str());
            }
            case WriteAheadLog.CHANGE_VISIBILITY -> {
                MessageQueue q = queues.get(in.str());
                if (q != null) q.applyChangeVisibility(in.str(), in.lng());
            }
            case WriteAheadLog.MOVE_TO_DLQ -> {
                MessageQueue source = queues.get(in.str());
                String id = in.str();
                MessageQueue dlq = queues.get(in.str());
                long visibleAt = in.lng();
                Message m = source == null ? null : source.applyRemove(id);
                if (m != null && dlq != null) dlq.applyAcceptMoved(m, visibleAt);
            }
            case WriteAheadLog.SNAPSHOT_MESSAGE -> {
                MessageQueue q = queues.get(in.str());
                if (q != null) q.applySnapshotMessage(in.str(), in.str(), in.lng(), in.lng(), in.i32(), in.str());
            }
            case WriteAheadLog.SNAPSHOT_DEDUP -> {
                MessageQueue q = queues.get(in.str());
                if (q != null) q.applyDedup(in.str(), in.str(), in.lng());
            }
            default -> throw new IllegalStateException("unknown WAL record type " + e.type());
        }
    }

    // ------------------------------------------------------------------ helpers

    private interface IoAction {
        void run() throws IOException;
    }

    private void withReadLock(IoAction action) throws IOException {
        stateLock.readLock().lock();
        try {
            action.run();
        } finally {
            stateLock.readLock().unlock();
        }
    }

    private MessageQueue require(String name) {
        MessageQueue q = name == null ? null : queues.get(name);
        if (q == null) throw QueueException.queueNotFound(name);
        return q;
    }

    private static void validateBody(String body) {
        if (body == null || body.isEmpty()) throw QueueException.badRequest("message body must not be empty");
        if (body.getBytes(StandardCharsets.UTF_8).length > MAX_BODY_BYTES) {
            throw QueueException.badRequest("message body is larger than 256 KB");
        }
    }

    private static byte[] encodeCreate(String name, QueueConfig c) {
        return new Codec.Out().str(name).lng(c.visibilityTimeoutMs()).i32(c.maxReceiveCount())
                .str(c.deadLetterQueue()).lng(c.delayMs()).toBytes();
    }
}
