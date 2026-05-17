package com.ai.learning.kafka.integration.service;

import com.ai.learning.kafka.integration.exception.DownstreamException;
import com.ai.learning.kafka.integration.exception.ServiceUnavailableException;
import com.ai.learning.kafka.integration.model.Response;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

@Service
public class RetrieveService {
    private final WebClient  webClient;
    public RetrieveService(WebClient webClient) {
        this.webClient = webClient;
    }
    public Mono<Response> retrieve(String topic) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/retrieve-by-topic")
                        .queryParam("topic",topic)
                        .build())
                .retrieve()
                .onStatus(
                        status -> status.is4xxClientError() || status.is5xxServerError(),
                        response -> Mono.error(new DownstreamException(response.statusCode().value()))
                )
                .bodyToMono(Response.class)
                .onErrorMap(
                        WebClientRequestException.class,
                        ex -> new ServiceUnavailableException("Search service is unavailable")
                );
    }
}
