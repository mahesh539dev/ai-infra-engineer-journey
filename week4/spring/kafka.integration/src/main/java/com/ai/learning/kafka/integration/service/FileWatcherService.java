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