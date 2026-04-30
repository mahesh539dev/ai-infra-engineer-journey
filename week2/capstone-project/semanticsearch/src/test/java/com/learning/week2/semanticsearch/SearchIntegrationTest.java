package com.learning.week2.semanticsearch;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.io.IOException;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SearchIntegrationTest {

    static MockWebServer mockWebServer;

    @LocalServerPort
    private int port;

    private WebTestClient webTestClient;

    @org.junit.jupiter.api.BeforeEach
    void initWebClient() {
        webTestClient = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
    }

    @BeforeAll
    static void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
    }

    @AfterAll
    static void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("search.api.base-url", () -> mockWebServer.url("/").toString());
    }

    @Test
    void search_returns200_withValidQueryTextAndNumResults() {
        mockWebServer.enqueue(new MockResponse()
                .setBody("""
                        {
                          "results": {
                            "result_1": {
                              "text": "sample text",
                              "metadata": { "chunk": 2, "page": 1 },
                              "similarity": 0.6338
                            }
                          }
                        }
                        """)
                .addHeader("Content-Type", "application/json"));

        webTestClient.get()
                .uri("/api/search?query_text=AI&num_results=1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.results.result_1.text").isEqualTo("sample text")
                .jsonPath("$.results.result_1.similarity").isEqualTo(0.6338)
                .jsonPath("$.results.result_1.metadata.chunk").isEqualTo(2);
    }

    @Test
    void search_returns200_withDefaultNumResults_whenNotProvided() {
        mockWebServer.enqueue(new MockResponse()
                .setBody("{\"results\":{}}")
                .addHeader("Content-Type", "application/json"));

        webTestClient.get()
                .uri("/api/search?query_text=AI")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void search_returns400_whenQueryTextMissing() {
        webTestClient.get()
                .uri("/api/search?num_results=3")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.status").isEqualTo(400)
                .jsonPath("$.error").isEqualTo("Bad Request")
                .jsonPath("$.message").isEqualTo("query_text must not be blank");
    }

    @Test
    void search_returns400_whenQueryTextIsBlank() {
        webTestClient.get()
                .uri("/api/search?query_text=   &num_results=3")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message").isEqualTo("query_text must not be blank");
    }

    @Test
    void search_returns400_whenNumResultsIsNotAnInteger() {
        webTestClient.get()
                .uri("/api/search?query_text=AI&num_results=abc")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message").isEqualTo("num_results must be a valid integer");
    }

    @Test
    void search_returns400_whenNumResultsIsLessThanOne() {
        webTestClient.get()
                .uri("/api/search?query_text=AI&num_results=0")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message").isEqualTo("num_results must be at least 1");
    }

    @Test
    void search_returns502_whenDownstreamReturns500() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        webTestClient.get()
                .uri("/api/search?query_text=AI")
                .exchange()
                .expectStatus().isEqualTo(502)
                .expectBody()
                .jsonPath("$.status").isEqualTo(502)
                .jsonPath("$.error").isEqualTo("Bad Gateway")
                .jsonPath("$.message").isEqualTo("Downstream error: 500");
    }

    @Test
    void search_returns502_whenDownstreamReturns404() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(404));

        webTestClient.get()
                .uri("/api/search?query_text=AI")
                .exchange()
                .expectStatus().isEqualTo(502)
                .expectBody()
                .jsonPath("$.status").isEqualTo(502)
                .jsonPath("$.error").isEqualTo("Bad Gateway")
                .jsonPath("$.message").isEqualTo("Downstream error: 404");
    }
}
