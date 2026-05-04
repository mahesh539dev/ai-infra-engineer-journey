package com.ai.learning.kafka.integration.component;

import com.ai.learning.kafka.integration.model.KafkaEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

    Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    private final StreamBridge streamBridge;

    public KafkaProducer(StreamBridge streamBridge){
        this.streamBridge = streamBridge;
    }

    public void sendMessage(KafkaEvent kafkaEvent){
        Message<KafkaEvent>  message= MessageBuilder.withPayload(kafkaEvent).build();
        streamBridge.send("ai-producer",message);
    }
}
