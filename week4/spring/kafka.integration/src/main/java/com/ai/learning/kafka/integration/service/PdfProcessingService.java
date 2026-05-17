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
        logger.info("Extracted {} chars, produced {} chunks", text.length(), chunks.size());
        if (chunks.isEmpty()) {
            logger.warn("No chunks extracted from {}", filePath);
            return;
        }

        String traceId    = UUID.randomUUID().toString();
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
        int maxAttempts  = props.retry().maxAttempts();
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