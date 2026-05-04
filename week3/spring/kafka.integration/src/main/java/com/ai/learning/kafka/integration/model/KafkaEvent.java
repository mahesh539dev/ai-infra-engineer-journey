package com.ai.learning.kafka.integration.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaEvent {

    @JsonProperty("message") private String message;
    @JsonProperty("topic") private String topic;
    @JsonProperty("subtopic") private String subtopic;

    public String getMessage() { return message; }
    public String getTopic() { return topic; }
    public String getSubtopic() { return subtopic; }

    @Override
    public String toString() {
        return "KafkaEvent{" +
                "message='" + message + '\'' +
                ", topic='" + topic + '\'' +
                ", subtopic='" + subtopic + '\'' +
                '}';
    }
}
