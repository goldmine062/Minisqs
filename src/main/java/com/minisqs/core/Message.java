package com.minisqs.core;

/**
 * A message stored in a queue.
 *
 * <p>The mutable fields (visibleAt, receiveCount, receiptHandle) are only read or written while
 * holding the owning queue's lock. visibleAt is also the sort key of the queue's schedule, so a
 * message must be removed from the schedule before visibleAt changes and re-added afterwards.
 */
final class Message {
    final String id;
    final String body;
    final long sentAt;
    /** Tie-breaker so messages with the same visibleAt keep their arrival order. */
    final long seq;

    /** Epoch millis at which the message can be received. In the future = delayed or in flight. */
    long visibleAt;
    int receiveCount;
    /** Handle of the latest receive, or null if the message was never received. */
    String receiptHandle;

    Message(String id, String body, long sentAt, long visibleAt, long seq) {
        this.id = id;
        this.body = body;
        this.sentAt = sentAt;
        this.visibleAt = visibleAt;
        this.seq = seq;
    }
}
