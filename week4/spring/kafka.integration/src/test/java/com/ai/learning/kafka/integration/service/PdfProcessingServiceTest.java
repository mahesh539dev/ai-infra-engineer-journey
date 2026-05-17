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