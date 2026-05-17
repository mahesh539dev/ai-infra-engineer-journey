package com.ai.learning.kafka.integration.component;

import com.ai.learning.kafka.integration.model.ChunkEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import java.util.List;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KafkaProducerChunkTest {

    @Mock StreamBridge streamBridge;
    KafkaProducer producer;

    @BeforeEach
    void setUp() {
        producer = new KafkaProducer(streamBridge);
    }

    @Test
    void sendChunkEventUsesRagProducerBinding() {
        ChunkEvent event = new ChunkEvent(
            "evt-1", "document_chunk", "1.0", "2026-05-11T00:00:00Z", "req-1", "trace-1",
            new ChunkEvent.Document("doc-1", "test.pdf", "pdf", "/test.pdf"),
            new ChunkEvent.Chunk("c-1", 1, 1, "text", 4),
            new ChunkEvent.Processing("recursive_character", 1000, 200),
            new ChunkEvent.Pinecone("index", "default"),
            new ChunkEvent.Metadata(List.of(), "en")
        );

        producer.sendChunkEvent(event);

        verify(streamBridge).send(eq("rag-producer"), any(Message.class));
    }
}