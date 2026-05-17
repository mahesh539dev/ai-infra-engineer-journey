# PDF File Watcher Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Watch a configurable directory for PDF files, extract and chunk their text using recursive character splitting, publish each chunk as a `ChunkEvent` to a new `rag-topic` Kafka binding, then move the file to `processed/` on success or `error/` after exhausted retries.

**Architecture:** A `FileWatcherService` runs a Java NIO `WatchService` on a background thread and performs a startup scan for existing PDFs. Each detected PDF is handed to `PdfProcessingService`, which extracts text via Apache PDFBox, splits it with `RecursiveCharacterTextSplitter`, builds a `ChunkEvent` per chunk, and publishes to Kafka via a dedicated `rag-producer` binding.

**Tech Stack:** Spring Boot 4.0.6 · WebFlux · Spring Cloud Stream · Apache PDFBox 3.0.3 · JUnit 5 · Mockito

---

## File Map

| Action | Path |
|--------|------|
| Modify | `pom.xml` |
| Modify | `src/main/resources/application.yaml` |
| Create | `src/main/java/com/ai/learning/kafka/integration/config/FileWatcherProperties.java` |
| Create | `src/main/java/com/ai/learning/kafka/integration/config/FileWatcherConfig.java` |
| Create | `src/main/java/com/ai/learning/kafka/integration/model/ChunkEvent.java` |
| Modify | `src/main/java/com/ai/learning/kafka/integration/component/KafkaProducer.java` |
| Create | `src/main/java/com/ai/learning/kafka/integration/util/RecursiveCharacterTextSplitter.java` |
| Create | `src/main/java/com/ai/learning/kafka/integration/service/PdfProcessingService.java` |
| Create | `src/main/java/com/ai/learning/kafka/integration/service/FileWatcherService.java` |
| Create | `src/test/java/com/ai/learning/kafka/integration/model/ChunkEventTest.java` |
| Create | `src/test/java/com/ai/learning/kafka/integration/component/KafkaProducerChunkTest.java` |
| Create | `src/test/java/com/ai/learning/kafka/integration/util/RecursiveCharacterTextSplitterTest.java` |
| Create | `src/test/java/com/ai/learning/kafka/integration/service/PdfProcessingServiceTest.java` |
| Create | `src/test/java/com/ai/learning/kafka/integration/service/FileWatcherServiceTest.java` |

---

## Task 1: Add PDFBox, Kafka binding, and Config Properties

**Files:**
- Modify: `pom.xml`
- Modify: `src/main/resources/application.yaml`
- Create: `src/main/java/com/ai/learning/kafka/integration/config/FileWatcherProperties.java`
- Create: `src/main/java/com/ai/learning/kafka/integration/config/FileWatcherConfig.java`

- [ ] **Step 1: Add PDFBox dependency to `pom.xml`**

  Add inside `<dependencies>`, after the jackson-databind entry:

  ```xml
  <dependency>
      <groupId>org.apache.pdfbox</groupId>
      <artifactId>pdfbox</artifactId>
      <version>3.0.3</version>
  </dependency>
  ```

- [ ] **Step 2: Add `rag-producer` binding and `file.watcher` config to `application.yaml`**

  Replace the full `application.yaml` with:

  ```yaml
  spring:
    application:
      name: kafka.integration
    cloud:
      stream:
        bindings:
          ai-producer:
            destination: learning-ai
            producer:
              useNativeEncoding: true
          rag-producer:
            destination: rag-topic
            producer:
              useNativeEncoding: true
        kafka:
          binder:
            producerProperties:
              key.serializer: org.apache.kafka.common.serialization.StringSerializer
              value.serializer: org.springframework.kafka.support.serializer.JsonSerializer
              acks: all
            brokers: localhost:9092
  search:
    api:
      base-url: localhost:8000
  server:
    port: 8090
  file:
    watcher:
      directory: ./watched-files
      chunk-size: 1000
      chunk-overlap: 200
      retry:
        max-attempts: 3
        delay-seconds: 5
      pinecone:
        index-name: infra-knowledge
        namespace: default
      metadata:
        language: en
        tags: []
  ```

- [ ] **Step 3: Create `FileWatcherProperties.java`**

  ```java
  package com.ai.learning.kafka.integration.config;

  import org.springframework.boot.context.properties.ConfigurationProperties;
  import java.util.List;

  @ConfigurationProperties(prefix = "file.watcher")
  public record FileWatcherProperties(
      String directory,
      int chunkSize,
      int chunkOverlap,
      Retry retry,
      Pinecone pinecone,
      Metadata metadata
  ) {
      public record Retry(int maxAttempts, int delaySeconds) {}
      public record Pinecone(String indexName, String namespace) {}
      public record Metadata(String language, List<String> tags) {}
  }
  ```

- [ ] **Step 4: Create `FileWatcherConfig.java`**

  ```java
  package com.ai.learning.kafka.integration.config;

  import org.springframework.boot.context.properties.EnableConfigurationProperties;
  import org.springframework.context.annotation.Configuration;

  @Configuration
  @EnableConfigurationProperties(FileWatcherProperties.class)
  public class FileWatcherConfig {}
  ```

- [ ] **Step 5: Verify the app compiles**

  ```
  mvn compile
  ```

  Expected: `BUILD SUCCESS`

- [ ] **Step 6: Commit**

  ```
  git add pom.xml src/main/resources/application.yaml
  git add src/main/java/com/ai/learning/kafka/integration/config/FileWatcherProperties.java
  git add src/main/java/com/ai/learning/kafka/integration/config/FileWatcherConfig.java
  git commit -m "feat: add PDFBox, rag-topic binding, and FileWatcherProperties config"
  ```

---

## Task 2: ChunkEvent Model

**Files:**
- Create: `src/main/java/com/ai/learning/kafka/integration/model/ChunkEvent.java`
- Create: `src/test/java/com/ai/learning/kafka/integration/model/ChunkEventTest.java`

- [ ] **Step 1: Write the failing test**

  ```java
  package com.ai.learning.kafka.integration.model;

  import com.fasterxml.jackson.databind.ObjectMapper;
  import org.junit.jupiter.api.Test;
  import java.util.List;
  import static org.assertj.core.api.Assertions.assertThat;

  class ChunkEventTest {

      private final ObjectMapper mapper = new ObjectMapper();

      @Test
      void serializesToSnakeCaseJson() throws Exception {
          ChunkEvent event = new ChunkEvent(
              "evt-1", "document_chunk", "1.0", "2026-05-11T00:00:00Z",
              "req-1", "trace-1",
              new ChunkEvent.Document("doc-1", "kafka-guide.pdf", "pdf", "/uploads/kafka-guide.pdf"),
              new ChunkEvent.Chunk("chunk-1", 1, 5, "Kafka retries...", 16),
              new ChunkEvent.Processing("recursive_character", 1000, 200),
              new ChunkEvent.Pinecone("infra-knowledge", "default"),
              new ChunkEvent.Metadata(List.of("kafka"), "en")
          );

          String json = mapper.writeValueAsString(event);

          assertThat(json).contains("\"event_id\"");
          assertThat(json).contains("\"event_type\"");
          assertThat(json).contains("\"event_version\"");
          assertThat(json).contains("\"request_id\"");
          assertThat(json).contains("\"trace_id\"");
          assertThat(json).contains("\"document_id\"");
          assertThat(json).contains("\"document_name\"");
          assertThat(json).contains("\"document_type\"");
          assertThat(json).contains("\"source_path\"");
          assertThat(json).contains("\"chunk_id\"");
          assertThat(json).contains("\"chunk_index\"");
          assertThat(json).contains("\"total_chunks\"");
          assertThat(json).contains("\"character_count\"");
          assertThat(json).contains("\"chunking_strategy\"");
          assertThat(json).contains("\"chunk_size\"");
          assertThat(json).contains("\"chunk_overlap\"");
          assertThat(json).contains("\"index_name\"");
          assertThat(json).doesNotContain("\"eventId\"");
          assertThat(json).doesNotContain("\"chunkIndex\"");
          assertThat(json).doesNotContain("\"totalChunks\"");
      }
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```
  mvn test -Dtest=ChunkEventTest
  ```

  Expected: FAIL — `ChunkEvent` does not exist yet.

- [ ] **Step 3: Create `ChunkEvent.java`**

  ```java
  package com.ai.learning.kafka.integration.model;

  import com.fasterxml.jackson.annotation.JsonProperty;
  import java.util.List;

  public record ChunkEvent(
      @JsonProperty("event_id")      String eventId,
      @JsonProperty("event_type")    String eventType,
      @JsonProperty("event_version") String eventVersion,
      @JsonProperty("timestamp")     String timestamp,
      @JsonProperty("request_id")    String requestId,
      @JsonProperty("trace_id")      String traceId,
      @JsonProperty("document")      Document document,
      @JsonProperty("chunk")         Chunk chunk,
      @JsonProperty("processing")    Processing processing,
      @JsonProperty("pinecone")      Pinecone pinecone,
      @JsonProperty("metadata")      Metadata metadata
  ) {
      public record Document(
          @JsonProperty("document_id")   String documentId,
          @JsonProperty("document_name") String documentName,
          @JsonProperty("document_type") String documentType,
          @JsonProperty("source_path")   String sourcePath
      ) {}

      public record Chunk(
          @JsonProperty("chunk_id")        String chunkId,
          @JsonProperty("chunk_index")     int chunkIndex,
          @JsonProperty("total_chunks")    int totalChunks,
          @JsonProperty("text")            String text,
          @JsonProperty("character_count") int characterCount
      ) {}

      public record Processing(
          @JsonProperty("chunking_strategy") String chunkingStrategy,
          @JsonProperty("chunk_size")        int chunkSize,
          @JsonProperty("chunk_overlap")     int chunkOverlap
      ) {}

      public record Pinecone(
          @JsonProperty("index_name") String indexName,
          @JsonProperty("namespace")  String namespace
      ) {}

      public record Metadata(
          @JsonProperty("tags")     List<String> tags,
          @JsonProperty("language") String language
      ) {}
  }
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```
  mvn test -Dtest=ChunkEventTest
  ```

  Expected: PASS

- [ ] **Step 5: Commit**

  ```
  git add src/main/java/com/ai/learning/kafka/integration/model/ChunkEvent.java
  git add src/test/java/com/ai/learning/kafka/integration/model/ChunkEventTest.java
  git commit -m "feat: add ChunkEvent model with snake_case JSON serialization"
  ```

---

## Task 3: RecursiveCharacterTextSplitter

**Files:**
- Create: `src/main/java/com/ai/learning/kafka/integration/util/RecursiveCharacterTextSplitter.java`
- Create: `src/test/java/com/ai/learning/kafka/integration/util/RecursiveCharacterTextSplitterTest.java`

- [ ] **Step 1: Write the failing tests**

  ```java
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
  ```

- [ ] **Step 2: Run tests to verify they fail**

  ```
  mvn test -Dtest=RecursiveCharacterTextSplitterTest
  ```

  Expected: FAIL — class does not exist.

- [ ] **Step 3: Create `RecursiveCharacterTextSplitter.java`**

  ```java
  package com.ai.learning.kafka.integration.util;

  import java.util.ArrayList;
  import java.util.List;
  import java.util.regex.Pattern;

  public class RecursiveCharacterTextSplitter {

      private static final List<String> SEPARATORS = List.of("\n\n", "\n", " ", "");

      private final int chunkSize;
      private final int chunkOverlap;

      public RecursiveCharacterTextSplitter(int chunkSize, int chunkOverlap) {
          this.chunkSize = chunkSize;
          this.chunkOverlap = chunkOverlap;
      }

      public List<String> split(String text) {
          if (text == null || text.isBlank()) return List.of();
          List<String> result = new ArrayList<>();
          doSplit(text.strip(), SEPARATORS, result);
          return result;
      }

      private void doSplit(String text, List<String> separators, List<String> output) {
          if (text.length() <= chunkSize) {
              output.add(text);
              return;
          }

          String chosenSep = "";
          int chosenIdx = separators.size() - 1;
          for (int i = 0; i < separators.size(); i++) {
              String s = separators.get(i);
              if (s.isEmpty() || text.contains(s)) {
                  chosenSep = s;
                  chosenIdx = i;
                  break;
              }
          }

          List<String> nextSeps = chosenIdx + 1 < separators.size()
                  ? separators.subList(chosenIdx + 1, separators.size())
                  : List.of();

          String[] parts = chosenSep.isEmpty()
                  ? splitByChar(text)
                  : text.split(Pattern.quote(chosenSep), -1);

          List<String> goodParts = new ArrayList<>();
          for (String part : parts) {
              String p = part.strip();
              if (p.isEmpty()) continue;
              if (p.length() > chunkSize) {
                  mergeInto(goodParts, chosenSep, output);
                  goodParts.clear();
                  doSplit(p, nextSeps.isEmpty() ? List.of("") : nextSeps, output);
              } else {
                  goodParts.add(p);
              }
          }
          mergeInto(goodParts, chosenSep, output);
      }

      private String[] splitByChar(String text) {
          List<String> parts = new ArrayList<>();
          for (int i = 0; i < text.length(); i += chunkSize - chunkOverlap) {
              parts.add(text.substring(i, Math.min(i + chunkSize, text.length())));
          }
          return parts.toArray(String[]::new);
      }

      private void mergeInto(List<String> parts, String sep, List<String> output) {
          if (parts.isEmpty()) return;
          int sepLen = sep.length();
          List<String> current = new ArrayList<>();

          for (String part : parts) {
              int projected = totalLen(current, sepLen) + (current.isEmpty() ? 0 : sepLen) + part.length();
              if (projected > chunkSize && !current.isEmpty()) {
                  output.add(String.join(sep, current));
                  while (!current.isEmpty() && totalLen(current, sepLen) > chunkOverlap) {
                      current.remove(0);
                  }
              }
              current.add(part);
          }
          if (!current.isEmpty()) output.add(String.join(sep, current));
      }

      private int totalLen(List<String> parts, int sepLen) {
          if (parts.isEmpty()) return 0;
          return parts.stream().mapToInt(String::length).sum() + Math.max(0, parts.size() - 1) * sepLen;
      }
  }
  ```

- [ ] **Step 4: Run tests to verify they pass**

  ```
  mvn test -Dtest=RecursiveCharacterTextSplitterTest
  ```

  Expected: 6 tests PASS

- [ ] **Step 5: Commit**

  ```
  git add src/main/java/com/ai/learning/kafka/integration/util/RecursiveCharacterTextSplitter.java
  git add src/test/java/com/ai/learning/kafka/integration/util/RecursiveCharacterTextSplitterTest.java
  git commit -m "feat: add RecursiveCharacterTextSplitter with overlap and paragraph-first strategy"
  ```

---

## Task 4: Update KafkaProducer for ChunkEvent

**Files:**
- Modify: `src/main/java/com/ai/learning/kafka/integration/component/KafkaProducer.java`
- Create: `src/test/java/com/ai/learning/kafka/integration/component/KafkaProducerChunkTest.java`

- [ ] **Step 1: Write the failing test**

  ```java
  package com.ai.learning.kafka.integration.component;

  import com.ai.learning.kafka.integration.model.ChunkEvent;
  import org.junit.jupiter.api.BeforeEach;
  import org.junit.jupiter.api.Test;
  import org.junit.jupiter.api.extension.ExtendWith;
  import org.mockito.Mock;
  import org.mockito.junit.jupiter.MockitoExtension;
  import org.springframework.cloud.stream.function.StreamBridge;
  import org.springframework.messaging.Message;
  import java.util.List;
  import static org.mockito.ArgumentMatchers.any;
  import static org.mockito.ArgumentMatchers.eq;
  import static org.mockito.Mockito.verify;

  @ExtendWith(MockitoExtension.class)
  class KafkaProducerChunkTest {

      @Mock StreamBridge streamBridge;
      KafkaProducer producer;

      @BeforeEach
      void setUp() {
          producer = new KafkaProducer(streamBridge);
      }

      @Test
      void sendChunkEventUsesRagProducerBinding() {
          ChunkEvent event = new ChunkEvent(
              "evt-1", "document_chunk", "1.0", "2026-05-11T00:00:00Z", "req-1", "trace-1",
              new ChunkEvent.Document("doc-1", "test.pdf", "pdf", "/test.pdf"),
              new ChunkEvent.Chunk("c-1", 1, 1, "text", 4),
              new ChunkEvent.Processing("recursive_character", 1000, 200),
              new ChunkEvent.Pinecone("index", "default"),
              new ChunkEvent.Metadata(List.of(), "en")
          );

          producer.sendChunkEvent(event);

          verify(streamBridge).send(eq("rag-producer"), any(Message.class));
      }
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```
  mvn test -Dtest=KafkaProducerChunkTest
  ```

  Expected: FAIL — `sendChunkEvent` method does not exist.

- [ ] **Step 3: Add `sendChunkEvent` to `KafkaProducer.java`**

  Add the following import at the top of the file (after existing imports):

  ```java
  import com.ai.learning.kafka.integration.model.ChunkEvent;
  ```

  Add the following method after `sendMessage`:

  ```java
  public void sendChunkEvent(ChunkEvent chunkEvent) {
      Message<ChunkEvent> message = MessageBuilder.withPayload(chunkEvent).build();
      streamBridge.send("rag-producer", message);
  }
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```
  mvn test -Dtest=KafkaProducerChunkTest
  ```

  Expected: PASS

- [ ] **Step 5: Commit**

  ```
  git add src/main/java/com/ai/learning/kafka/integration/component/KafkaProducer.java
  git add src/test/java/com/ai/learning/kafka/integration/component/KafkaProducerChunkTest.java
  git commit -m "feat: add sendChunkEvent method to KafkaProducer targeting rag-producer binding"
  ```

---

## Task 5: PdfProcessingService

**Files:**
- Create: `src/main/java/com/ai/learning/kafka/integration/service/PdfProcessingService.java`
- Create: `src/test/java/com/ai/learning/kafka/integration/service/PdfProcessingServiceTest.java`

- [ ] **Step 1: Write the failing tests**

  ```java
  package com.ai.learning.kafka.integration.service;

  import com.ai.learning.kafka.integration.component.KafkaProducer;
  import com.ai.learning.kafka.integration.config.FileWatcherProperties;
  import com.ai.learning.kafka.integration.model.ChunkEvent;
  import org.apache.pdfbox.pdmodel.PDDocument;
  import org.apache.pdfbox.pdmodel.PDPage;
  import org.apache.pdfbox.pdmodel.PDPageContentStream;
  import org.apache.pdfbox.pdmodel.font.PDType1Font;
  import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
  import org.junit.jupiter.api.BeforeEach;
  import org.junit.jupiter.api.Test;
  import org.junit.jupiter.api.extension.ExtendWith;
  import org.junit.jupiter.api.io.TempDir;
  import org.mockito.Mock;
  import org.mockito.junit.jupiter.MockitoExtension;

  import java.io.IOException;
  import java.nio.file.Files;
  import java.nio.file.Path;
  import java.util.List;

  import static org.assertj.core.api.Assertions.assertThat;
  import static org.mockito.ArgumentMatchers.any;
  import static org.mockito.Mockito.*;

  @ExtendWith(MockitoExtension.class)
  class PdfProcessingServiceTest {

      @Mock KafkaProducer kafkaProducer;
      @TempDir Path tempDir;

      private PdfProcessingService service;

      @BeforeEach
      void setUp() {
          FileWatcherProperties props = new FileWatcherProperties(
              tempDir.toString(), 100, 20,
              new FileWatcherProperties.Retry(3, 0),
              new FileWatcherProperties.Pinecone("test-index", "default"),
              new FileWatcherProperties.Metadata("en", List.of("test"))
          );
          service = new PdfProcessingService(kafkaProducer, props);
      }

      @Test
      void publishesChunksAndMovesToProcessed() throws Exception {
          Path pdf = createTestPdf("Hello world. ".repeat(15));

          service.process(pdf);

          verify(kafkaProducer, atLeastOnce()).sendChunkEvent(any(ChunkEvent.class));
          assertThat(tempDir.resolve("processed").resolve(pdf.getFileName())).exists();
          assertThat(pdf).doesNotExist();
      }

      @Test
      void chunkEventsHaveCorrectDocumentName() throws Exception {
          Path pdf = createTestPdf("Hello world. ".repeat(15));

          service.process(pdf);

          var captor = org.mockito.ArgumentCaptor.forClass(ChunkEvent.class);
          verify(kafkaProducer, atLeastOnce()).sendChunkEvent(captor.capture());
          assertThat(captor.getValue().document().documentName())
              .isEqualTo(pdf.getFileName().toString());
          assertThat(captor.getValue().document().documentType()).isEqualTo("pdf");
          assertThat(captor.getValue().processing().chunkingStrategy())
              .isEqualTo("recursive_character");
      }

      @Test
      void allChunksShareSameTraceId() throws Exception {
          Path pdf = createTestPdf("Hello world. ".repeat(15));

          service.process(pdf);

          var captor = org.mockito.ArgumentCaptor.forClass(ChunkEvent.class);
          verify(kafkaProducer, atLeastOnce()).sendChunkEvent(captor.capture());
          List<ChunkEvent> events = captor.getAllValues();
          if (events.size() > 1) {
              String traceId = events.get(0).traceId();
              assertThat(events).allMatch(e -> e.traceId().equals(traceId));
          }
      }

      @Test
      void corruptPdfMovesToErrorAfterExhaustedRetries() throws Exception {
          Path corrupt = tempDir.resolve("corrupt.pdf");
          Files.writeString(corrupt, "not a pdf");

          FileWatcherProperties fastRetry = new FileWatcherProperties(
              tempDir.toString(), 100, 20,
              new FileWatcherProperties.Retry(1, 0),
              new FileWatcherProperties.Pinecone("test-index", "default"),
              new FileWatcherProperties.Metadata("en", List.of())
          );
          PdfProcessingService svc = new PdfProcessingService(kafkaProducer, fastRetry);

          svc.process(corrupt);

          verify(kafkaProducer, never()).sendChunkEvent(any());
          assertThat(tempDir.resolve("error").resolve("corrupt.pdf")).exists();
          assertThat(corrupt).doesNotExist();
      }

      @Test
      void kafkaFailureLeavesFileInPlace() throws Exception {
          Path pdf = createTestPdf("Hello world. ".repeat(15));
          doThrow(new RuntimeException("Kafka down")).when(kafkaProducer).sendChunkEvent(any());

          service.process(pdf);

          assertThat(pdf).exists();
          assertThat(tempDir.resolve("processed")).doesNotExist();
      }

      private Path createTestPdf(String content) throws IOException {
          PDDocument doc = new PDDocument();
          PDPage page = new PDPage();
          doc.addPage(page);
          PDType1Font font = new PDType1Font(Standard14Fonts.FontName.HELVETICA);
          try (PDPageContentStream cs = new PDPageContentStream(doc, page)) {
              cs.beginText();
              cs.setFont(font, 12);
              cs.setLeading(14.5f);
              cs.newLineAtOffset(25, 700);
              for (int i = 0; i < content.length(); i += 60) {
                  cs.showText(content.substring(i, Math.min(i + 60, content.length())));
                  cs.newLine();
              }
              cs.endText();
          }
          Path path = tempDir.resolve("test-" + System.nanoTime() + ".pdf");
          doc.save(path.toFile());
          doc.close();
          return path;
      }
  }
  ```

- [ ] **Step 2: Run tests to verify they fail**

  ```
  mvn test -Dtest=PdfProcessingServiceTest
  ```

  Expected: FAIL — `PdfProcessingService` does not exist.

- [ ] **Step 3: Create `PdfProcessingService.java`**

  ```java
  package com.ai.learning.kafka.integration.service;

  import com.ai.learning.kafka.integration.component.KafkaProducer;
  import com.ai.learning.kafka.integration.config.FileWatcherProperties;
  import com.ai.learning.kafka.integration.model.ChunkEvent;
  import com.ai.learning.kafka.integration.util.RecursiveCharacterTextSplitter;
  import org.apache.pdfbox.Loader;
  import org.apache.pdfbox.pdmodel.PDDocument;
  import org.apache.pdfbox.text.PDFTextStripper;
  import org.slf4j.Logger;
  import org.slf4j.LoggerFactory;
  import org.springframework.stereotype.Service;

  import java.io.IOException;
  import java.nio.file.Files;
  import java.nio.file.Path;
  import java.time.Instant;
  import java.util.List;
  import java.util.UUID;

  @Service
  public class PdfProcessingService {

      private static final Logger logger = LoggerFactory.getLogger(PdfProcessingService.class);

      private final KafkaProducer kafkaProducer;
      private final FileWatcherProperties props;
      private final RecursiveCharacterTextSplitter splitter;

      public PdfProcessingService(KafkaProducer kafkaProducer, FileWatcherProperties props) {
          this.kafkaProducer = kafkaProducer;
          this.props = props;
          this.splitter = new RecursiveCharacterTextSplitter(props.chunkSize(), props.chunkOverlap());
      }

      public void process(Path filePath) {
          logger.info("Processing PDF: {}", filePath);
          String text = extractTextWithRetry(filePath);
          if (text == null) return;

          List<String> chunks = splitter.split(text);
          if (chunks.isEmpty()) {
              logger.warn("No chunks extracted from {}", filePath);
              return;
          }

          String traceId  = UUID.randomUUID().toString();
          String requestId  = UUID.randomUUID().toString();
          String documentId = UUID.randomUUID().toString();
          String documentName = filePath.getFileName().toString();
          int totalChunks = chunks.size();

          for (int i = 0; i < chunks.size(); i++) {
              String chunkText = chunks.get(i);
              ChunkEvent event = buildEvent(traceId, requestId, documentId,
                      documentName, filePath.toAbsolutePath().toString(),
                      i + 1, totalChunks, chunkText);
              try {
                  kafkaProducer.sendChunkEvent(event);
              } catch (Exception e) {
                  logger.error("Kafka publish failed at chunk {}/{} for {}", i + 1, totalChunks, filePath, e);
                  return;
              }
          }

          logger.info("Published {} chunks for {}", totalChunks, filePath);
          moveFile(filePath, "processed");
      }

      private String extractTextWithRetry(Path filePath) {
          int maxAttempts = props.retry().maxAttempts();
          int delaySeconds = props.retry().delaySeconds();

          for (int attempt = 1; attempt <= maxAttempts; attempt++) {
              try (PDDocument doc = Loader.loadPDF(filePath.toFile())) {
                  return new PDFTextStripper().getText(doc);
              } catch (IOException e) {
                  logger.warn("PDF extraction attempt {}/{} failed for {}: {}",
                          attempt, maxAttempts, filePath, e.getMessage());
                  if (attempt < maxAttempts && delaySeconds > 0) {
                      try { Thread.sleep(delaySeconds * 1000L); }
                      catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                  }
              }
          }

          logger.error("All {} extraction attempts exhausted for {}", maxAttempts, filePath);
          moveFile(filePath, "error");
          return null;
      }

      private ChunkEvent buildEvent(String traceId, String requestId, String documentId,
              String documentName, String sourcePath,
              int chunkIndex, int totalChunks, String text) {
          return new ChunkEvent(
              UUID.randomUUID().toString(),
              "document_chunk",
              "1.0",
              Instant.now().toString(),
              requestId,
              traceId,
              new ChunkEvent.Document(documentId, documentName, "pdf", sourcePath),
              new ChunkEvent.Chunk(UUID.randomUUID().toString(), chunkIndex, totalChunks,
                      text, text.length()),
              new ChunkEvent.Processing("recursive_character", props.chunkSize(), props.chunkOverlap()),
              new ChunkEvent.Pinecone(props.pinecone().indexName(), props.pinecone().namespace()),
              new ChunkEvent.Metadata(props.metadata().tags(), props.metadata().language())
          );
      }

      private void moveFile(Path filePath, String subdir) {
          try {
              Path target = filePath.getParent().resolve(subdir);
              Files.createDirectories(target);
              Files.move(filePath, target.resolve(filePath.getFileName()));
              logger.info("Moved {} to {}/", filePath.getFileName(), subdir);
          } catch (IOException e) {
              logger.warn("Failed to move {} to {}/: {}", filePath.getFileName(), subdir, e.getMessage());
          }
      }
  }
  ```

- [ ] **Step 4: Run tests to verify they pass**

  ```
  mvn test -Dtest=PdfProcessingServiceTest
  ```

  Expected: 5 tests PASS

- [ ] **Step 5: Commit**

  ```
  git add src/main/java/com/ai/learning/kafka/integration/service/PdfProcessingService.java
  git add src/test/java/com/ai/learning/kafka/integration/service/PdfProcessingServiceTest.java
  git commit -m "feat: add PdfProcessingService with retry, chunk publishing, and file lifecycle"
  ```

---

## Task 6: FileWatcherService

**Files:**
- Create: `src/main/java/com/ai/learning/kafka/integration/service/FileWatcherService.java`
- Create: `src/test/java/com/ai/learning/kafka/integration/service/FileWatcherServiceTest.java`

- [ ] **Step 1: Write the failing tests**

  ```java
  package com.ai.learning.kafka.integration.service;

  import com.ai.learning.kafka.integration.config.FileWatcherProperties;
  import org.junit.jupiter.api.Test;
  import org.junit.jupiter.api.extension.ExtendWith;
  import org.junit.jupiter.api.io.TempDir;
  import org.mockito.ArgumentCaptor;
  import org.mockito.Mock;
  import org.mockito.junit.jupiter.MockitoExtension;

  import java.nio.file.Files;
  import java.nio.file.Path;
  import java.util.List;
  import java.util.concurrent.CountDownLatch;
  import java.util.concurrent.TimeUnit;

  import static org.assertj.core.api.Assertions.assertThat;
  import static org.mockito.ArgumentMatchers.any;
  import static org.mockito.Mockito.*;

  @ExtendWith(MockitoExtension.class)
  class FileWatcherServiceTest {

      @Mock PdfProcessingService pdfProcessingService;
      @TempDir Path tempDir;

      @Test
      void startupScanProcessesExistingPdfs() throws Exception {
          Files.createFile(tempDir.resolve("doc1.pdf"));
          Files.createFile(tempDir.resolve("doc2.pdf"));
          Files.createFile(tempDir.resolve("readme.txt")); // must be ignored

          FileWatcherService svc = new FileWatcherService(pdfProcessingService, makeProps());
          svc.start();

          ArgumentCaptor<Path> captor = ArgumentCaptor.forClass(Path.class);
          verify(pdfProcessingService, times(2)).process(captor.capture());
          assertThat(captor.getAllValues())
              .extracting(p -> p.getFileName().toString())
              .containsExactlyInAnyOrder("doc1.pdf", "doc2.pdf");

          svc.stop();
      }

      @Test
      void newPdfFileTriggersProcessing() throws Exception {
          CountDownLatch latch = new CountDownLatch(1);
          doAnswer(inv -> { latch.countDown(); return null; })
              .when(pdfProcessingService).process(any(Path.class));

          FileWatcherService svc = new FileWatcherService(pdfProcessingService, makeProps());
          svc.start();
          Thread.sleep(200); // let WatchService register

          Files.createFile(tempDir.resolve("new.pdf"));

          assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
          svc.stop();
      }

      @Test
      void nonPdfFilesAreIgnored() throws Exception {
          FileWatcherService svc = new FileWatcherService(pdfProcessingService, makeProps());
          svc.start();
          Thread.sleep(200);

          Files.createFile(tempDir.resolve("report.txt"));
          Files.createFile(tempDir.resolve("data.csv"));

          Thread.sleep(500);
          verify(pdfProcessingService, never()).process(any(Path.class));
          svc.stop();
      }

      private FileWatcherProperties makeProps() {
          return new FileWatcherProperties(
              tempDir.toString(), 100, 20,
              new FileWatcherProperties.Retry(3, 0),
              new FileWatcherProperties.Pinecone("test-index", "default"),
              new FileWatcherProperties.Metadata("en", List.of())
          );
      }
  }
  ```

- [ ] **Step 2: Run tests to verify they fail**

  ```
  mvn test -Dtest=FileWatcherServiceTest
  ```

  Expected: FAIL — `FileWatcherService` does not exist.

- [ ] **Step 3: Create `FileWatcherService.java`**

  ```java
  package com.ai.learning.kafka.integration.service;

  import com.ai.learning.kafka.integration.config.FileWatcherProperties;
  import jakarta.annotation.PostConstruct;
  import jakarta.annotation.PreDestroy;
  import org.slf4j.Logger;
  import org.slf4j.LoggerFactory;
  import org.springframework.stereotype.Service;

  import java.io.IOException;
  import java.nio.file.*;
  import java.util.concurrent.ExecutorService;
  import java.util.concurrent.Executors;

  @Service
  public class FileWatcherService {

      private static final Logger logger = LoggerFactory.getLogger(FileWatcherService.class);

      private final PdfProcessingService pdfProcessingService;
      private final FileWatcherProperties props;
      private WatchService watchService;
      private final ExecutorService executor = Executors.newSingleThreadExecutor();

      public FileWatcherService(PdfProcessingService pdfProcessingService, FileWatcherProperties props) {
          this.pdfProcessingService = pdfProcessingService;
          this.props = props;
      }

      @PostConstruct
      public void start() throws IOException {
          Path watchDir = Path.of(props.directory());
          Files.createDirectories(watchDir);

          try (DirectoryStream<Path> existing = Files.newDirectoryStream(watchDir, "*.pdf")) {
              for (Path pdf : existing) {
                  pdfProcessingService.process(pdf);
              }
          }

          watchService = FileSystems.getDefault().newWatchService();
          watchDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

          executor.submit(() -> {
              while (!Thread.currentThread().isInterrupted()) {
                  try {
                      WatchKey key = watchService.take();
                      for (WatchEvent<?> event : key.pollEvents()) {
                          if (event.kind() == StandardWatchEventKinds.OVERFLOW) continue;
                          Path filename = (Path) event.context();
                          if (filename.toString().endsWith(".pdf")) {
                              pdfProcessingService.process(watchDir.resolve(filename));
                          }
                      }
                      key.reset();
                  } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                  }
              }
          });

          logger.info("File watcher started on: {}", watchDir.toAbsolutePath());
      }

      @PreDestroy
      public void stop() throws IOException {
          executor.shutdownNow();
          if (watchService != null) watchService.close();
      }
  }
  ```

- [ ] **Step 4: Run tests to verify they pass**

  ```
  mvn test -Dtest=FileWatcherServiceTest
  ```

  Expected: 3 tests PASS

- [ ] **Step 5: Run the full test suite**

  ```
  mvn test
  ```

  Expected: ALL tests PASS (includes LoadDataBulkTest + all new tests)

- [ ] **Step 6: Commit**

  ```
  git add src/main/java/com/ai/learning/kafka/integration/service/FileWatcherService.java
  git add src/test/java/com/ai/learning/kafka/integration/service/FileWatcherServiceTest.java
  git commit -m "feat: add FileWatcherService with startup scan and WatchService event loop"
  ```