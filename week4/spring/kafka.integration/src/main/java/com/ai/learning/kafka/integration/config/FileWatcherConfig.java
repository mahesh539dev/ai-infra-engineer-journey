package com.ai.learning.kafka.integration.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(FileWatcherProperties.class)
public class FileWatcherConfig {}