package com.ai.learning.kafka.integration.handler;

import com.ai.learning.kafka.integration.component.KafkaProducer;
import com.ai.learning.kafka.integration.exception.ErrorResponse;
import com.ai.learning.kafka.integration.exception.ServiceUnavailableException;
import com.ai.learning.kafka.integration.exception.ValidationException;
import com.ai.learning.kafka.integration.model.KafkaEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.codec.DecodingException;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;

import org.springframework.core.ParameterizedTypeReference;

import java.util.List;
import java.util.Map;

@Component
public class LoadData {

    private static final Logger logger = LoggerFactory.getLogger(LoadData.class);

    private final KafkaProducer kafkaProducer;

    public LoadData(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public Mono<ServerResponse> sendDataToKafka(ServerRequest request) {
        return request.bodyToMono(KafkaEvent.class)
                .onErrorMap(e -> e instanceof DecodingException || e instanceof ServerWebInputException,
                        e -> new ValidationException("Invalid request body: malformed JSON"))
                .switchIfEmpty(Mono.error(new ValidationException("Request body must not be empty")))
                .doOnNext(this::validate)
                .flatMap(kafkaEvent -> {
                    try {
                        kafkaProducer.sendMessage(kafkaEvent);
                        logger.info("Successfully published event to Kafka: {}", kafkaEvent);
                        return ServerResponse.ok()
                                .contentType(MediaType.APPLICATION_JSON)
                                .bodyValue(Map.of("status", "success", "message", "Event published to Kafka"));
                    } catch (Exception e) {
                        logger.error("Kafka send failed", e);
                        return Mono.error(new ServiceUnavailableException("Kafka unavailable: " + e.getMessage()));
                    }
                })
                .onErrorResume(ValidationException.class, e ->
                        ServerResponse.badRequest()
                                .contentType(MediaType.APPLICATION_JSON)
                                .bodyValue(new ErrorResponse(400, "Bad Request", e.getMessage())))
                .onErrorResume(ServiceUnavailableException.class, e ->
                        ServerResponse.status(503)
                                .contentType(MediaType.APPLICATION_JSON)
                                .bodyValue(new ErrorResponse(503, "Service Unavailable", e.getMessage())))
                .onErrorResume(e -> {
                    logger.error("Unexpected error in loaddata handler", e);
                    return ServerResponse.status(500)
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(new ErrorResponse(500, "Internal Server Error", "An unexpected error occurred"));
                });
    }

    private void validate(KafkaEvent event) {
        if (event.getMessage() == null || event.getMessage().isBlank()) {
            throw new ValidationException("message must not be blank");
        }
        if (event.getTopic() == null || event.getTopic().isBlank()) {
            throw new ValidationException("topic must not be blank");
        }
        if(event.getSubtopic() == null || event.getSubtopic().isBlank()){
            throw new ValidationException("subtopic must not be blank");
        }
    }

    public Mono<ServerResponse> sendBulkDataToKafka(ServerRequest request) {
        return request.bodyToMono(new ParameterizedTypeReference<List<KafkaEvent>>() {})
                .onErrorMap(e -> e instanceof DecodingException || e instanceof ServerWebInputException,
                        e -> new ValidationException("Invalid request body: malformed JSON"))
                .switchIfEmpty(Mono.error(new ValidationException("Request body must not be empty")))
                .flatMap(events -> {
                    if (events.isEmpty()) {
                        return Mono.error(new ValidationException("Event list must not be empty"));
                    }
                    for (int i = 0; i < events.size(); i++) {
                        try {
                            validate(events.get(i));
                        } catch (ValidationException e) {
                            return Mono.error(new ValidationException("Event[" + i + "]: " + e.getMessage()));
                        }
                    }
                    try {
                        events.forEach(kafkaProducer::sendMessage);
                        logger.info("Successfully published {} events to Kafka", events.size());
                        return ServerResponse.ok()
                                .contentType(MediaType.APPLICATION_JSON)
                                .bodyValue(Map.of(
                                        "status", "success",
                                        "message", "Events published to Kafka",
                                        "count", events.size()
                                ));
                    } catch (Exception e) {
                        logger.error("Kafka bulk send failed", e);
                        return Mono.error(new ServiceUnavailableException("Kafka unavailable: " + e.getMessage()));
                    }
                })
                .onErrorResume(ValidationException.class, e ->
                        ServerResponse.badRequest()
                                .contentType(MediaType.APPLICATION_JSON)
                                .bodyValue(new ErrorResponse(400, "Bad Request", e.getMessage())))
                .onErrorResume(ServiceUnavailableException.class, e ->
                        ServerResponse.status(503)
                                .contentType(MediaType.APPLICATION_JSON)
                                .bodyValue(new ErrorResponse(503, "Service Unavailable", e.getMessage())))
                .onErrorResume(e -> {
                    logger.error("Unexpected error in bulk loaddata handler", e);
                    return ServerResponse.status(500)
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(new ErrorResponse(500, "Internal Server Error", "An unexpected error occurred"));
                });

    }
}