package com.learning.week2.semanticsearch.handler;

import com.learning.week2.semanticsearch.exception.ValidationException;
import com.learning.week2.semanticsearch.service.SearchService;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.util.Optional;

@Component
public class SearchHandler {

    private final SearchService searchService;

    public SearchHandler(SearchService searchService) {
        this.searchService = searchService;
    }

    public Mono<ServerResponse> search(ServerRequest request) {
        String queryText = request.queryParam("query_text")
                .filter(s -> !s.isBlank())
                .orElseThrow(() -> new ValidationException("query_text must not be blank"));

        int numResults = resolveNumResults(request.queryParam("num_results"));

        return searchService.search(queryText, numResults)
                .flatMap(response -> ServerResponse.ok()
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(response));
    }

    private int resolveNumResults(Optional<String> param) {
        if (param.isEmpty()) {
            return 5;
        }
        int value;
        try {
            value = Integer.parseInt(param.get());
        } catch (NumberFormatException e) {
            throw new ValidationException("num_results must be a valid integer");
        }
        if (value < 1) {
            throw new ValidationException("num_results must be at least 1");
        }
        return value;
    }
}
