package com.learning.week2.semanticsearch.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SearchResponseDeserializationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void deserializesSearchResponseFromJson() throws Exception {
        String json = """
            {
              "results": {
                "result_1": {
                  "text": "sample text",
                  "metadata": { "chunk": 2, "page": 1 },
                  "similarity": 0.6338
                },
                "result_2": {
                  "text": "another text",
                  "metadata": { "chunk": 0, "page": 1 },
                  "similarity": 0.6068
                }
              }
            }
            """;

        SearchResponse response = objectMapper.readValue(json, SearchResponse.class);

        assertThat(response.results()).hasSize(2);

        ResultEntry result1 = response.results().get("result_1");
        assertThat(result1.text()).isEqualTo("sample text");
        assertThat(result1.metadata().chunk()).isEqualTo(2);
        assertThat(result1.metadata().page()).isEqualTo(1);
        assertThat(result1.similarity()).isEqualTo(0.6338);

        ResultEntry result2 = response.results().get("result_2");
        assertThat(result2.text()).isEqualTo("another text");
        assertThat(result2.similarity()).isEqualTo(0.6068);
    }
}
