package com.ai.learning.kafka.integration.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record ChunkEvent(
    @JsonProperty("event_id")      String eventId,
    @JsonProperty("event_type")    String eventType,
    @JsonProperty("event_version") String eventVersion,
    @JsonProperty("timestamp")     String timestamp,
    @JsonProperty("request_id")    String requestId,
    @JsonProperty("trace_id")      String traceId,
    @JsonProperty("document")      Document document,
    @JsonProperty("chunk")         Chunk chunk,
    @JsonProperty("processing")    Processing processing,
    @JsonProperty("pinecone")      Pinecone pinecone,
    @JsonProperty("metadata")      Metadata metadata
) {
    public record Document(
        @JsonProperty("document_id")   String documentId,
        @JsonProperty("document_name") String documentName,
        @JsonProperty("document_type") String documentType,
        @JsonProperty("source_path")   String sourcePath
    ) {}

    public record Chunk(
        @JsonProperty("chunk_id")        String chunkId,
        @JsonProperty("chunk_index")     int chunkIndex,
        @JsonProperty("total_chunks")    int totalChunks,
        @JsonProperty("text")            String text,
        @JsonProperty("character_count") int characterCount
    ) {}

    public record Processing(
        @JsonProperty("chunking_strategy") String chunkingStrategy,
        @JsonProperty("chunk_size")        int chunkSize,
        @JsonProperty("chunk_overlap")     int chunkOverlap
    ) {}

    public record Pinecone(
        @JsonProperty("index_name") String indexName,
        @JsonProperty("namespace")  String namespace
    ) {}

    public record Metadata(
        @JsonProperty("tags")     List<String> tags,
        @JsonProperty("language") String language
    ) {}
}
