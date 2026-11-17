package com.minisqs.core;

/** What a consumer gets back from a receive call. */
public record ReceivedMessage(String messageId, String receiptHandle, String body, int receiveCount, long sentAt) {}
