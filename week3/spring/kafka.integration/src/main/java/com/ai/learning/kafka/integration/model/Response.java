package com.ai.learning.kafka.integration.model;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public record Response (
        @JsonProperty("topic_results")
        Map<String,ResultEntry> topicResults
) {}