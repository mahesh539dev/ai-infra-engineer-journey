package com.learning.week2.semanticsearch.service;

import com.learning.week2.semanticsearch.exception.DownstreamException;
import com.learning.week2.semanticsearch.exception.ServiceUnavailableException;
import com.learning.week2.semanticsearch.model.SearchResponse;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.test.StepVerifier;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class SearchServiceTest {

    private MockWebServer mockWebServer;
    private SearchService searchService;

    @BeforeEach
    void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        WebClient webClient = WebClient.builder()
                .baseUrl(mockWebServer.url("/").toString())
                .build();
        searchService = new SearchService(webClient);
    }

    @AfterEach
    void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    void search_returnsResults_whenDownstreamSucceeds() throws InterruptedException {
        String responseBody = """
                {
                  "results": {
                    "result_1": {
                      "text": "sample text",
                      "metadata": { "chunk": 2, "page": 1 },
                      "similarity": 0.6338
                    }
                  }
                }
                """;
        mockWebServer.enqueue(new MockResponse()
                .setBody(responseBody)
                .addHeader("Content-Type", "application/json"));

        StepVerifier.create(searchService.search("AI healthcare", 1))
                .assertNext(response -> {
                    assertThat(response.results()).hasSize(1);
                    assertThat(response.results().get("result_1").text()).isEqualTo("sample text");
                    assertThat(response.results().get("result_1").similarity()).isEqualTo(0.6338);
                    assertThat(response.results().get("result_1").metadata().chunk()).isEqualTo(2);
                })
                .verifyComplete();

        RecordedRequest request = mockWebServer.takeRequest();
        assertThat(request.getPath()).contains("query_text=AI%20healthcare");
        assertThat(request.getPath()).contains("num_results=1");
        assertThat(request.getPath()).startsWith("/search-semantic");
    }

    @Test
    void search_usesDefaultNumResults_whenFive() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse()
                .setBody("{\"results\":{}}")
                .addHeader("Content-Type", "application/json"));

        StepVerifier.create(searchService.search("query", 5))
                .assertNext(response -> assertThat(response.results()).isEmpty())
                .verifyComplete();

        RecordedRequest request = mockWebServer.takeRequest();
        assertThat(request.getPath()).contains("num_results=5");
    }

    @Test
    void search_throwsDownstreamException_onDownstream4xx() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(404));

        StepVerifier.create(searchService.search("query", 5))
                .expectErrorMatches(ex ->
                        ex instanceof DownstreamException &&
                        ((DownstreamException) ex).getStatusCode() == 404)
                .verify();
    }

    @Test
    void search_throwsDownstreamException_onDownstream5xx() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        StepVerifier.create(searchService.search("query", 5))
                .expectErrorMatches(ex ->
                        ex instanceof DownstreamException &&
                        ((DownstreamException) ex).getStatusCode() == 500)
                .verify();
    }

    @Test
    void search_throwsServiceUnavailableException_onConnectionFailure() {
        // Shut down the server to simulate connection refused
        try {
            mockWebServer.shutdown();
        } catch (IOException e) {
            // ignore
        }

        StepVerifier.create(searchService.search("query", 5))
                .expectErrorMatches(ex -> ex instanceof ServiceUnavailableException &&
                        ex.getMessage().equals("Search service is unavailable"))
                .verify();
    }
}
