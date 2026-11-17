package com.minisqs.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Tiny binary encoder/decoder for WAL record payloads. */
final class Codec {
    private Codec() {}

    static final class Out {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream(128);
        private final DataOutputStream out = new DataOutputStream(bytes);

        Out str(String s) {
            try {
                if (s == null) {
                    out.writeInt(-1);
                } else {
                    byte[] b = s.getBytes(UTF_8);
                    out.writeInt(b.length);
                    out.write(b);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return this;
        }

        Out lng(long v) {
            try {
                out.writeLong(v);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return this;
        }

        Out i32(int v) {
            try {
                out.writeInt(v);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return this;
        }

        byte[] toBytes() {
            return bytes.toByteArray();
        }
    }

    static final class In {
        private final DataInputStream in;

        In(byte[] payload) {
            this.in = new DataInputStream(new ByteArrayInputStream(payload));
        }

        String str() {
            try {
                int n = in.readInt();
                if (n < 0) return null;
                byte[] b = new byte[n];
                in.readFully(b);
                return new String(b, UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        long lng() {
            try {
                return in.readLong();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        int i32() {
            try {
                return in.readInt();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
