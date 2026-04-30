package com.learning.week2.semanticsearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public record SearchResponse(
        @JsonProperty("results") Map<String, ResultEntry> results
) {}
