package com.maersk.kafkautility.service;

import com.maersk.kafkautility.annotations.RetryHandler;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Slf4j
@Component
@NoArgsConstructor
public class RetryService <T> {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private KafkaTemplate<String, T> kafkaTemplate;

    @Autowired
    private MessagePublishHandler<T> messagePublishHandler;

    private static final String RETRY_TOPIC_PLACEHOLDER = "${kafka.retry.topic}";

    @RetryHandler
    public void sendMessageToRetryTopic(T message, T aggregateId) {
        log.info("Inside retry service method");
    }
}
