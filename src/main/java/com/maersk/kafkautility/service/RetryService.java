package com.maersk.kafkautility.service;

import com.microsoft.azure.storage.StorageException;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.util.Objects;

@Slf4j
@Component
@NoArgsConstructor
public class RetryService <T extends Serializable> {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private KafkaTemplate<String, T> kafkaTemplate;

    @Autowired
    private MessagePublishHandler<T> messagePublishHandler;

    @Autowired
    private AzureBlobService<T> azureBlobService;

    private static final String RETRY_TOPIC_PLACEHOLDER = "${kafka.retry.topic}";

    public void sendMessageToRetryTopic(T message, T aggregateId) throws URISyntaxException, InvalidKeyException, StorageException {
        String payloadReference = null;
        try {
            var retryTopic = context.getEnvironment().resolvePlaceholders(RETRY_TOPIC_PLACEHOLDER);
            payloadReference = azureBlobService.writePayloadToBlob(message);
            log.info("retryTopic: {}", retryTopic);
                log.info("message to publish: {}", message);
                ProducerRecord<String, T> producerRecord = new ProducerRecord<>(retryTopic, (T)payloadReference);
                var kafkaHeaders = producerRecord.headers();
                if (Objects.nonNull(aggregateId)) {
                    kafkaHeaders.add("X-DOCBROKER-Correlation-ID", aggregateId.toString().getBytes(StandardCharsets.UTF_8));
                }
                messagePublishHandler.publishOnTopic(producerRecord);
        } catch (Exception ex)
        {
            log.error("Exception in retry advice ", ex);
            azureBlobService.deletePayloadFromBlob(payloadReference);
        }
    }
}
