package com.minisqs.core;

/** A point-in-time view of a queue, used by the API, dashboard and metrics. */
public record QueueStats(
        String name,
        QueueConfig config,
        long visible,
        long inFlight,
        long delayed,
        long oldestVisibleAgeMs,
        long sent,
        long received,
        long deleted,
        long movedToDlq) {}
