package com.ai.learning.kafka.integration.router;

import com.ai.learning.kafka.integration.handler.LoadData;
import com.ai.learning.kafka.integration.handler.RetrieveInfo;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.*;


@Configuration
public class ApiRouter {

    @Bean
    public RouterFunction<ServerResponse> loadDataRoute(LoadData loadData) {
        return RouterFunctions.route(
                RequestPredicates.POST("/api/loaddata")
                        .and(RequestPredicates.contentType(MediaType.APPLICATION_JSON)),
                loadData::sendDataToKafka
        );
    }

    @Bean
    public RouterFunction<ServerResponse> loadDataBulkRoute(LoadData loadData) {
        return RouterFunctions.route(
                RequestPredicates.POST("/api/loaddatabulk")
                        .and(RequestPredicates.contentType(MediaType.APPLICATION_JSON)),
                loadData::sendBulkDataToKafka
        );
    }

    @Bean
    public RouterFunction<ServerResponse> retrieveData(RetrieveInfo retrieveInfo) {
        return RouterFunctions.route(
                RequestPredicates.GET("/api/retrieve"),
                retrieveInfo::retrieveData
        );
    }
}