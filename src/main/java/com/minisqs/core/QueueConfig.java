package com.minisqs.core;

/**
 * Per-queue settings.
 *
 * @param visibilityTimeoutMs how long a received message stays hidden before it can be received again
 * @param maxReceiveCount     receives allowed before the message moves to the dead-letter queue (0 = never)
 * @param deadLetterQueue     name of the dead-letter queue, or null
 * @param delayMs             default delay before a newly sent message becomes visible
 */
public record QueueConfig(long visibilityTimeoutMs, int maxReceiveCount, String deadLetterQueue, long delayMs) {

    public static final long MAX_VISIBILITY_TIMEOUT_MS = 12 * 60 * 60 * 1000L; // 12 hours, same as SQS
    public static final long MAX_DELAY_MS = 15 * 60 * 1000L;                     // 15 minutes, same as SQS

    public QueueConfig {
        if (visibilityTimeoutMs < 0 || visibilityTimeoutMs > MAX_VISIBILITY_TIMEOUT_MS) {
            throw QueueException.badRequest("visibilityTimeout must be between 0 and 43200 seconds");
        }
        if (maxReceiveCount < 0) {
            throw QueueException.badRequest("maxReceiveCount must be >= 0");
        }
        if (delayMs < 0 || delayMs > MAX_DELAY_MS) {
            throw QueueException.badRequest("delay must be between 0 and 900 seconds");
        }
        if (deadLetterQueue != null && deadLetterQueue.isBlank()) {
            deadLetterQueue = null;
        }
        if (maxReceiveCount > 0 && deadLetterQueue == null) {
            throw QueueException.badRequest("maxReceiveCount needs a dlq (dead-letter queue)");
        }
    }

    public static QueueConfig defaults() {
        return new QueueConfig(30_000, 0, null, 0);
    }
}
