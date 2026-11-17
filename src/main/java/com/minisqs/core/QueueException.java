package com.minisqs.core;

/** An error caused by the request (bad input, unknown queue, stale receipt handle...). */
public final class QueueException extends RuntimeException {
    public final int status;
    public final String code;

    public QueueException(int status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public static QueueException badRequest(String message) {
        return new QueueException(400, "InvalidParameter", message);
    }

    public static QueueException queueNotFound(String name) {
        return new QueueException(404, "QueueDoesNotExist", "queue '" + name + "' does not exist");
    }

    public static QueueException invalidReceiptHandle() {
        return new QueueException(410, "ReceiptHandleIsInvalid",
                "receipt handle is no longer valid: the message was deleted or received again by another consumer");
    }

    public static QueueException conflict(String message) {
        return new QueueException(409, "Conflict", message);
    }
}
