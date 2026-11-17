package com.minisqs.core;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.zip.CRC32;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Append-only log of every state change. The in-memory queues are rebuilt by replaying it on startup.
 *
 * <p>Record layout on disk:
 * <pre>
 *   [length: int32][crc32: int32][type: 1 byte][payload: length-1 bytes]
 * </pre>
 * length covers type + payload; the CRC covers type + payload. A crash in the middle of a write leaves a
 * torn record at the tail; replay detects it (short read or CRC mismatch) and truncates the file there.
 *
 * <p>Durability uses group commit: many threads append (cheap, goes to the OS page cache), then call
 * {@link #sync(long)}. One thread does the expensive fsync and every thread whose record was already
 * written before that fsync started returns without doing its own.
 */
public final class WriteAheadLog implements Closeable {
    static final byte CREATE_QUEUE = 1;
    static final byte DELETE_QUEUE = 2;
    static final byte SEND = 3;
    static final byte RECEIVE = 4;
    static final byte DELETE = 5;
    static final byte CHANGE_VISIBILITY = 6;
    static final byte MOVE_TO_DLQ = 7;
    static final byte SNAPSHOT_MESSAGE = 8;
    static final byte SNAPSHOT_DEDUP = 9;

    private static final int PREFIX = 8;            // length + crc
    private static final int MAX_RECORD = 16 << 20; // sanity bound when reading a possibly corrupt length

    record Entry(byte type, byte[] payload) {}

    private final Path path;
    private final boolean fsyncEnabled;
    private final Object syncLock = new Object();
    private final AtomicLong fsyncs = new AtomicLong();
    private final AtomicLong appends = new AtomicLong();

    private FileChannel channel;
    private long writePos;    // guarded by this
    private long syncedPos;   // guarded by syncLock
    private long truncatedBytes;

    public WriteAheadLog(Path path, boolean fsyncEnabled) throws IOException {
        this.path = path;
        this.fsyncEnabled = fsyncEnabled;
        this.channel = FileChannel.open(path, CREATE, READ, WRITE);
    }

    /**
     * Feeds every intact record to the consumer, in order. Stops at the first torn or corrupt record
     * and truncates the file at that point so new appends start from a clean tail.
     *
     * @return number of records replayed
     */
    int replay(Consumer<Entry> consumer) throws IOException {
        long size = channel.size();
        long pos = 0;
        int count = 0;
        ByteBuffer prefix = ByteBuffer.allocate(PREFIX);
        while (pos + PREFIX < size) {
            prefix.clear();
            readFully(prefix, pos);
            prefix.flip();
            int length = prefix.getInt();
            int crc = prefix.getInt();
            if (length < 1 || length > MAX_RECORD || pos + PREFIX + length > size) {
                break; // torn write at the tail
            }
            ByteBuffer data = ByteBuffer.allocate(length);
            readFully(data, pos + PREFIX);
            byte[] bytes = data.array();
            CRC32 c = new CRC32();
            c.update(bytes, 0, bytes.length);
            if ((int) c.getValue() != crc) {
                break; // corrupt record
            }
            byte[] payload = new byte[length - 1];
            System.arraycopy(bytes, 1, payload, 0, payload.length);
            consumer.accept(new Entry(bytes[0], payload));
            pos += PREFIX + length;
            count++;
        }
        if (pos < size) {
            truncatedBytes = size - pos;
            channel.truncate(pos);
            channel.force(true);
        }
        synchronized (this) {
            writePos = pos;
        }
        synchronized (syncLock) {
            syncedPos = pos;
        }
        return count;
    }

    /** Appends one record (to the OS page cache, not yet durable). Returns the log position after it. */
    synchronized long append(byte type, byte[] payload) throws IOException {
        ByteBuffer buf = encode(type, payload);
        while (buf.hasRemaining()) {
            writePos += channel.write(buf, writePos);
        }
        appends.incrementAndGet();
        return writePos;
    }

    /** Blocks until everything up to {@code position} is on disk. No-op when fsync is disabled. */
    void sync(long position) throws IOException {
        if (!fsyncEnabled || position <= 0) return;
        synchronized (syncLock) {
            if (syncedPos >= position) {
                return; // someone else's fsync already covered our record (group commit)
            }
            long target;
            synchronized (this) {
                target = writePos;
            }
            channel.force(false);
            syncedPos = target;
            fsyncs.incrementAndGet();
        }
    }

    /** Atomically swaps the log for a compacted file (written by {@link #writeFile}). */
    void replaceWith(Path compacted) throws IOException {
        synchronized (syncLock) {
            synchronized (this) {
                channel.close();
                try {
                    Files.move(compacted, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                    fsyncDirectory(path.toAbsolutePath().getParent());
                } finally {
                    channel = FileChannel.open(path, CREATE, READ, WRITE);
                    writePos = channel.size();
                    syncedPos = writePos;
                }
            }
        }
    }

    /** Writes a complete log file (used by compaction) and fsyncs it. */
    static void writeFile(Path file, List<Entry> entries) throws IOException {
        try (FileChannel out = FileChannel.open(file, CREATE, WRITE, TRUNCATE_EXISTING)) {
            for (Entry e : entries) {
                ByteBuffer buf = encode(e.type(), e.payload());
                while (buf.hasRemaining()) {
                    out.write(buf);
                }
            }
            out.force(true);
        }
    }

    synchronized long sizeBytes() {
        return writePos;
    }

    long fsyncCount() {
        return fsyncs.get();
    }

    long appendCount() {
        return appends.get();
    }

    long truncatedBytesOnRecovery() {
        return truncatedBytes;
    }

    boolean fsyncEnabled() {
        return fsyncEnabled;
    }

    @Override
    public void close() throws IOException {
        synchronized (syncLock) {
            synchronized (this) {
                if (channel.isOpen()) {
                    channel.force(true);
                    channel.close();
                }
            }
        }
    }

    private static ByteBuffer encode(byte type, byte[] payload) {
        CRC32 crc = new CRC32();
        crc.update(type);
        crc.update(payload, 0, payload.length);
        ByteBuffer buf = ByteBuffer.allocate(PREFIX + 1 + payload.length);
        buf.putInt(1 + payload.length).putInt((int) crc.getValue()).put(type).put(payload).flip();
        return buf;
    }

    private void readFully(ByteBuffer buf, long position) throws IOException {
        long start = position;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, start + buf.position());
            if (n < 0) throw new EOFException();
        }
    }

    /** Makes the rename itself durable. Not supported on Windows, where it is skipped. */
    private static void fsyncDirectory(Path dir) {
        try (FileChannel d = FileChannel.open(dir, READ)) {
            d.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // best effort
        }
    }
}
