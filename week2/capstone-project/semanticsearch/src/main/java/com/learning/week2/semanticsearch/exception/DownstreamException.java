package com.learning.week2.semanticsearch.exception;

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
