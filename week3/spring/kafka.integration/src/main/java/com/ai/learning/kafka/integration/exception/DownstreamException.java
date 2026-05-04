package com.ai.learning.kafka.integration.exception;

public class DownstreamException extends RuntimeException {
    private final int statusCode;

    public DownstreamException(int statusCode) {
        super("Downstream error: " + statusCode);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
