package com.ai.learning.kafka.integration.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Metadata (
    @JsonProperty("topic")  String topic
) {}
