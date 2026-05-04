package com.ai.learning.kafka.integration.handler;

import com.ai.learning.kafka.integration.component.KafkaProducer;
import com.ai.learning.kafka.integration.model.KafkaEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunctions;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LoadDataBulkTest {

    @Mock
    private KafkaProducer kafkaProducer;

    private WebTestClient client;

    @BeforeEach
    void setup() {
        LoadData handler = new LoadData(kafkaProducer);
        client = WebTestClient.bindToRouterFunction(
            RouterFunctions.route(
                RequestPredicates.POST("/api/loaddatabulk")
                    .and(RequestPredicates.contentType(MediaType.APPLICATION_JSON)),
                handler::sendBulkDataToKafka
            )
        ).build();
    }

    @Test
    void bulkLoad_emptyList_returns400() {
        client.post().uri("/api/loaddatabulk")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(List.of())
            .exchange()
            .expectStatus().isBadRequest()
            .expectBody()
            .jsonPath("$.status").isEqualTo(400)
            .jsonPath("$.message").isEqualTo("Event list must not be empty");
    }

    @Test
    void bulkLoad_nonArrayBody_returns400MalformedJson() {
        // sending a JSON string instead of an array triggers DecodingException → 400
        client.post().uri("/api/loaddatabulk")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("not an array")
            .exchange()
            .expectStatus().isBadRequest()
            .expectBody()
            .jsonPath("$.status").isEqualTo(400)
            .jsonPath("$.message").isEqualTo("Invalid request body: malformed JSON");
    }

    @Test
    void bulkLoad_validEvents_sendsEachToKafka_returns200WithCount() {
        client.post().uri("/api/loaddatabulk")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(List.of(
                Map.of("message", "hello", "topic", "orders", "subtopic", "created"),
                Map.of("message", "world", "topic", "orders", "subtopic", "updated")
            ))
            .exchange()
            .expectStatus().isOk()
            .expectBody()
            .jsonPath("$.status").isEqualTo("success")
            .jsonPath("$.count").isEqualTo(2);

        verify(kafkaProducer, times(2)).sendMessage(any(KafkaEvent.class));
    }

    @Test
    void bulkLoad_firstEventMissingTopic_returns400WithEventIndex() {
        client.post().uri("/api/loaddatabulk")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(List.of(
                Map.of("message", "hello", "subtopic", "created")  // topic missing
            ))
            .exchange()
            .expectStatus().isBadRequest()
            .expectBody()
            .jsonPath("$.status").isEqualTo(400)
            .jsonPath("$.message").value(containsString("Event[0]"));
    }

    @Test
    void bulkLoad_secondEventMissingSubtopic_returns400WithCorrectIndex() {
        client.post().uri("/api/loaddatabulk")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(List.of(
                Map.of("message", "valid", "topic", "orders", "subtopic", "ok"),
                Map.of("message", "bad",   "topic", "orders")  // subtopic missing
            ))
            .exchange()
            .expectStatus().isBadRequest()
            .expectBody()
            .jsonPath("$.status").isEqualTo(400)
            .jsonPath("$.message").value(containsString("Event[1]"));
    }

    @Test
    void bulkLoad_kafkaThrows_returns503() {
        doThrow(new RuntimeException("broker down")).when(kafkaProducer).sendMessage(any());

        client.post().uri("/api/loaddatabulk")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(List.of(
                Map.of("message", "m", "topic", "t", "subtopic", "s")
            ))
            .exchange()
            .expectStatus().isEqualTo(503)
            .expectBody()
            .jsonPath("$.status").isEqualTo(503);

        verify(kafkaProducer).sendMessage(any(KafkaEvent.class));
    }
}