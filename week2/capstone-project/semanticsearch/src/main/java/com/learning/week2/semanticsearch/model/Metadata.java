package com.learning.week2.semanticsearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Metadata(
        @JsonProperty("chunk") int chunk,
        @JsonProperty("page") int page
) {}
