package com.ai.learning.kafka.integration.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class ChunkEventTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void serializesToSnakeCaseJson() throws Exception {
        ChunkEvent event = new ChunkEvent(
            "evt-1", "document_chunk", "1.0", "2026-05-11T00:00:00Z",
            "req-1", "trace-1",
            new ChunkEvent.Document("doc-1", "kafka-guide.pdf", "pdf", "/uploads/kafka-guide.pdf"),
            new ChunkEvent.Chunk("chunk-1", 1, 5, "Kafka retries...", 16),
            new ChunkEvent.Processing("recursive_character", 1000, 200),
            new ChunkEvent.Pinecone("infra-knowledge", "default"),
            new ChunkEvent.Metadata(List.of("kafka"), "en")
        );

        String json = mapper.writeValueAsString(event);

        assertThat(json).contains("\"event_id\"");
        assertThat(json).contains("\"event_type\"");
        assertThat(json).contains("\"event_version\"");
        assertThat(json).contains("\"request_id\"");
        assertThat(json).contains("\"trace_id\"");
        assertThat(json).contains("\"document_id\"");
        assertThat(json).contains("\"document_name\"");
        assertThat(json).contains("\"document_type\"");
        assertThat(json).contains("\"source_path\"");
        assertThat(json).contains("\"chunk_id\"");
        assertThat(json).contains("\"chunk_index\"");
        assertThat(json).contains("\"total_chunks\"");
        assertThat(json).contains("\"character_count\"");
        assertThat(json).contains("\"chunking_strategy\"");
        assertThat(json).contains("\"chunk_size\"");
        assertThat(json).contains("\"chunk_overlap\"");
        assertThat(json).contains("\"index_name\"");
        assertThat(json).doesNotContain("\"eventId\"");
        assertThat(json).doesNotContain("\"chunkIndex\"");
        assertThat(json).doesNotContain("\"totalChunks\"");
    }
}
