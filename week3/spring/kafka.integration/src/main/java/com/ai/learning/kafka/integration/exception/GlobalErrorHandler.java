package com.ai.learning.kafka.integration.exception;

import org.springframework.boot.autoconfigure.web.WebProperties;
import org.springframework.boot.webflux.autoconfigure.error.AbstractErrorWebExceptionHandler;
import org.springframework.boot.webflux.error.ErrorAttributes;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.*;
import org.springframework.web.reactive.resource.NoResourceFoundException;
import reactor.core.publisher.Mono;

@Component
@Order(-2)
public class GlobalErrorHandler extends AbstractErrorWebExceptionHandler {

    public GlobalErrorHandler(ErrorAttributes errorAttributes,
                              WebProperties webProperties,
                              ApplicationContext applicationContext,
                              ServerCodecConfigurer configurer) {
        super(errorAttributes, webProperties.getResources(), applicationContext);
        this.setMessageWriters(configurer.getWriters());
        this.setMessageReaders(configurer.getReaders());
    }

    @Override
    protected RouterFunction<ServerResponse> getRoutingFunction(ErrorAttributes errorAttributes) {
        return RouterFunctions.route(RequestPredicates.all(), this::renderErrorResponse);
    }

    private Mono<ServerResponse> renderErrorResponse(ServerRequest request) {
        Throwable error = getError(request);

        int status;
        String errorLabel;
        String message;

        if (error instanceof NoResourceFoundException) {
            status = 404;
            errorLabel = "Not Found";
            message = "Resource not found";
        } else if (error instanceof ValidationException) {
            status = 400;
            errorLabel = "Bad Request";
            message = error.getMessage();
        } else if (error instanceof DownstreamException de) {
            status = 502;
            errorLabel = "Bad Gateway";
            message = "Downstream error: " + de.getStatusCode();
        } else if (error instanceof ServiceUnavailableException) {
            status = 503;
            errorLabel = "Service Unavailable";
            message = error.getMessage();
        } else {
            status = 500;
            errorLabel = "Internal Server Error";
            message = "An unexpected error occurred";
        }

        return ServerResponse.status(status)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new ErrorResponse(status, errorLabel, message));
    }
}
