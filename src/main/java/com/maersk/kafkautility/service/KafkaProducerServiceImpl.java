package com.maersk.kafkautility.service;

import com.maersk.kafkautility.utils.AzureUtil;
import com.microsoft.azure.storage.StorageException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.openjdk.jol.vm.VM;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;

@Slf4j
@Service
public class KafkaProducerServiceImpl<T extends Serializable> implements KafkaProducerService<T> {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private AzureBlobService<T> azureBlobService;

    private static final String PRODUCER_TOPIC_NAME = "${kafka.notification.topic}";
    private static final String PAYLOAD_SIZE = "${events-payload.max-bytes}";

    @Override
    public ProducerRecord<String, String> getProducerRecord(T payload) throws URISyntaxException, IOException, InvalidKeyException, StorageException {
        try {
            String producerTopic = context.getEnvironment().resolvePlaceholders(PRODUCER_TOPIC_NAME);
            log.info("Topic name: {}", producerTopic);
            String storageReference = azureBlobService.writePayloadToBlob(payload);
            log.info("storageReference: {}", storageReference);
            return new ProducerRecord<>(producerTopic, storageReference);
        }
        catch (Exception e)
        {
          log.error("Exception while preparing Producer record", e);
          throw e;
        }
    }

    @Override
    public ProducerRecord<String, T> readProducerRecord(T payload) throws URISyntaxException, IOException, InvalidKeyException, StorageException {
        ProducerRecord<String, T> producerRecord = null;
        try {
            String producerTopic = context.getEnvironment().resolvePlaceholders(PRODUCER_TOPIC_NAME);
            if (isLargePayload(payload)) {
                log.info("Payload exceeds max configured size");
                var containerDest = AzureUtil.getCloudBlobContainer();
                var blobUri = azureBlobService.writePayloadFileToBlob(payload, containerDest);
                producerRecord = new ProducerRecord<>(producerTopic, (T) blobUri.toString());
                producerRecord.headers().add("isLargePayload", "YES".getBytes(StandardCharsets.UTF_8));
            } else {
                producerRecord = new ProducerRecord<>(producerTopic, payload);
                producerRecord.headers().add("isLargePayload", "NO".getBytes(StandardCharsets.UTF_8));
            }
            return producerRecord;
        }
        catch (Exception e)
        {
            log.error("Exception while preparing Producer record", e);
            throw e;
        }
    }

    private boolean isLargePayload(T payload)
    {
        var maxSize = context.getEnvironment().resolvePlaceholders(PAYLOAD_SIZE);
        var payloadSize = VM.current().sizeOf(payload);
        log.info("Payload size: {} bytes", payloadSize);
        return payloadSize > Long.parseLong(maxSize);
    }

}
