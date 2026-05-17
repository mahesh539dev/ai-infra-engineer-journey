package com.ai.learning.kafka.integration.util;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class RecursiveCharacterTextSplitterTest {

    @Test
    void nullAndBlankInputReturnsEmpty() {
        var s = new RecursiveCharacterTextSplitter(100, 20);
        assertThat(s.split(null)).isEmpty();
        assertThat(s.split("")).isEmpty();
        assertThat(s.split("   ")).isEmpty();
    }

    @Test
    void shortTextReturnedAsSingleChunk() {
        var s = new RecursiveCharacterTextSplitter(100, 20);
        List<String> chunks = s.split("Hello world");
        assertThat(chunks).hasSize(1);
        assertThat(chunks.get(0)).isEqualTo("Hello world");
    }

    @Test
    void allChunksWithinChunkSize() {
        var s = new RecursiveCharacterTextSplitter(50, 10);
        String text = "word ".repeat(40).strip();
        List<String> chunks = s.split(text);
        assertThat(chunks).isNotEmpty();
        assertThat(chunks).allMatch(c -> c.length() <= 50);
    }

    @Test
    void longTextProducesMultipleChunks() {
        var s = new RecursiveCharacterTextSplitter(50, 10);
        String text = "word ".repeat(40).strip();
        assertThat(s.split(text).size()).isGreaterThan(1);
    }

    @Test
    void paragraphSeparatorUsedBeforeNewline() {
        var s = new RecursiveCharacterTextSplitter(50, 10);
        String text = "First paragraph text.\n\nSecond paragraph text.";
        List<String> chunks = s.split(text);
        assertThat(chunks.get(0)).contains("First paragraph");
        assertThat(chunks).allMatch(c -> c.length() <= 50);
    }

    @Test
    void charLevelChunksHaveOverlap() {
        // chunkSize=10, overlap=3: splitting "abcdefghijklmnopqrstuvwxyz" char-by-char
        // chunk[0] = "abcdefghij", chunk[1] starts with "hij"
        var s = new RecursiveCharacterTextSplitter(10, 3);
        List<String> chunks = s.split("abcdefghijklmnopqrstuvwxyz");
        assertThat(chunks.size()).isGreaterThan(1);
        for (int i = 0; i < chunks.size() - 1; i++) {
            String tail = chunks.get(i).substring(chunks.get(i).length() - 3);
            assertThat(chunks.get(i + 1)).startsWith(tail);
        }
    }
}
