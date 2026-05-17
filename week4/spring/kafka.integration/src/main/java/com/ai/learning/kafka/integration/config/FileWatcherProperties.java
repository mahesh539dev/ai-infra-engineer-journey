package com.ai.learning.kafka.integration.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.List;

@ConfigurationProperties(prefix = "file.watcher")
public record FileWatcherProperties(
    String directory,
    int chunkSize,
    int chunkOverlap,
    Retry retry,
    Pinecone pinecone,
    Metadata metadata
) {
    public record Retry(int maxAttempts, int delaySeconds) {}
    public record Pinecone(String indexName, String namespace) {}
    public record Metadata(String language, List<String> tags) {}
}