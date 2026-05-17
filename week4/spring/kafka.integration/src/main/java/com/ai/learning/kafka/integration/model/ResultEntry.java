package com.ai.learning.kafka.integration.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ResultEntry(
        @JsonProperty("text") String text,
        @JsonProperty("metadata") Metadata metadata,
        @JsonProperty("similarity") double similarity
) {}