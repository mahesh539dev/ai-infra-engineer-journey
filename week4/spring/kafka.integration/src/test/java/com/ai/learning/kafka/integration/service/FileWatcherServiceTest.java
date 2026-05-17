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