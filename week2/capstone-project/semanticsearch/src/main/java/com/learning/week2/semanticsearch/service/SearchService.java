package com.learning.week2.semanticsearch.service;

import com.learning.week2.semanticsearch.exception.DownstreamException;
import com.learning.week2.semanticsearch.exception.ServiceUnavailableException;
import com.learning.week2.semanticsearch.model.SearchResponse;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Mono;

@Service
public class SearchService {

    private final WebClient webClient;

    public SearchService(WebClient webClient) {
        this.webClient = webClient;
    }

    public Mono<SearchResponse> search(String queryText, int numResults) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/search-semantic")
                        .queryParam("query_text", "{query_text}")
                        .queryParam("num_results", numResults)
                        .build(queryText))
                .retrieve()
                .onStatus(
                        status -> status.is4xxClientError() || status.is5xxServerError(),
                        response -> Mono.error(new DownstreamException(response.statusCode().value()))
                )
                .bodyToMono(SearchResponse.class)
                .onErrorMap(
                        WebClientRequestException.class,
                        ex -> new ServiceUnavailableException("Search service is unavailable")
                );
    }
}
