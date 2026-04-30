package com.learning.week2.semanticsearch.router;

import com.learning.week2.semanticsearch.exception.ErrorResponse;
import com.learning.week2.semanticsearch.handler.SearchHandler;
import com.learning.week2.semanticsearch.model.SearchResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springdoc.core.annotations.RouterOperation;
import org.springdoc.core.annotations.RouterOperations;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

@Configuration
public class SearchRouter {

    @Bean
    @RouterOperations({
        @RouterOperation(
            path = "/api/search",
            method = RequestMethod.GET,
            beanClass = SearchHandler.class,
            beanMethod = "search",
            operation = @Operation(
                operationId = "semanticSearch",
                summary = "Semantic Search",
                description = "Proxies a semantic search query to the external search service.",
                parameters = {
                    @Parameter(
                        name = "query_text",
                        in = ParameterIn.QUERY,
                        required = true,
                        description = "The search query string",
                        schema = @Schema(type = "string")
                    ),
                    @Parameter(
                        name = "num_results",
                        in = ParameterIn.QUERY,
                        required = false,
                        description = "Number of results to return (default: 5, minimum: 1)",
                        schema = @Schema(type = "integer", defaultValue = "5")
                    )
                },
                responses = {
                    @ApiResponse(
                        responseCode = "200",
                        description = "Search results returned successfully",
                        content = @Content(schema = @Schema(implementation = SearchResponse.class))
                    ),
                    @ApiResponse(
                        responseCode = "400",
                        description = "Missing or invalid query parameters",
                        content = @Content(schema = @Schema(implementation = ErrorResponse.class))
                    ),
                    @ApiResponse(
                        responseCode = "502",
                        description = "Downstream search service returned an error",
                        content = @Content(schema = @Schema(implementation = ErrorResponse.class))
                    ),
                    @ApiResponse(
                        responseCode = "503",
                        description = "Downstream search service is unreachable",
                        content = @Content(schema = @Schema(implementation = ErrorResponse.class))
                    )
                }
            )
        )
    })
    public RouterFunction<ServerResponse> searchRoutes(SearchHandler handler) {
        return RouterFunctions.route(
                RequestPredicates.GET("/api/search"),
                handler::search
        );
    }
}
