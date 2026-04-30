package com.learning.week2.semanticsearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ResultEntry(
        @JsonProperty("text") String text,
        @JsonProperty("metadata") Metadata metadata,
        @JsonProperty("similarity") double similarity
) {}
