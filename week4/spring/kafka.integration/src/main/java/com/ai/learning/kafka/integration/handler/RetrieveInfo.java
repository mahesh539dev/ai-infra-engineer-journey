package com.ai.learning.kafka.integration.handler;

import com.ai.learning.kafka.integration.exception.ValidationException;
import com.ai.learning.kafka.integration.service.RetrieveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

@Component
public class RetrieveInfo {
    private static final Logger logger = LoggerFactory.getLogger(RetrieveInfo.class);

    private  final RetrieveService retrieveService;

    public RetrieveInfo(RetrieveService retrieveService){
        this.retrieveService = retrieveService;
    }

    public Mono<ServerResponse> retrieveData(ServerRequest request) {
        String topicSearch = request.queryParam("topic")
                .filter(s->!s.isBlank())
                .orElseThrow(() -> new ValidationException("Topic in the search cannot be blank"));
        return retrieveService.retrieve(topicSearch)
                .flatMap(response -> ServerResponse.ok()
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(response));
    }
}
