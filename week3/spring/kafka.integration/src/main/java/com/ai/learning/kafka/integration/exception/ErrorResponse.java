package com.ai.learning.kafka.integration.exception;

public record ErrorResponse(int status, String error, String message) {}
