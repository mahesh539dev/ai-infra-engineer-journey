package com.learning.week2.semanticsearch.exception;

public record ErrorResponse(int status, String error, String message) {}
