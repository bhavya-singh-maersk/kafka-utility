package com.maersk.kafkautility.service;

import com.maersk.kafkautility.utils.AzureUtil;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
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
import java.util.UUID;

@Slf4j
@Service
public class KafkaProducerServiceImpl<T extends Serializable> implements KafkaProducerService<T> {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private AzureBlobService<T> azureBlobService;

    private static final String PRODUCER_TOPIC_NAME = "${kafka.notification.topic}";
    private static final String PAYLOAD_SIZE = "${events-payload.max-bytes}";
    private static final String PAYLOAD_FILE_NAME = "${events-payload.file-name}";
    private static final String AZURE_STORAGE_ACCOUNT_NAME = "${azure.storage.account-name}";
    private static final String AZURE_STORAGE_ACCOUNT_KEY = "${azure.storage.account-key}";
    private static final String AZURE_STORAGE_CONTAINER_NAME = "${azure.storage.container-name}";
    private static final String AZURE_STORAGE_ENDPOINT_SUFFIX = "${azure.storage.endpoint-suffix}";
    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=%s";

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
                var containerDest = getCloudBlobContainer();
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

    public CloudBlobContainer getCloudBlobContainer() throws StorageException, URISyntaxException, InvalidKeyException {
        String containerName = context.getEnvironment().resolvePlaceholders(AZURE_STORAGE_CONTAINER_NAME);
        log.info("containerName: {}", containerName);
        CloudStorageAccount storageAccountDest = CloudStorageAccount.parse(getConnectionString());
        CloudBlobClient blobClientDest = storageAccountDest.createCloudBlobClient();
        CloudBlobContainer containerDest = blobClientDest.getContainerReference(containerName);
        log.info("Container: {}", containerDest.getName());
        return containerDest;
    }

    private String getConnectionString()
    {
        String storageAccountName = context.getEnvironment().resolvePlaceholders(AZURE_STORAGE_ACCOUNT_NAME);
        String storageAccountKey = context.getEnvironment().resolvePlaceholders(AZURE_STORAGE_ACCOUNT_KEY);
        String endpointSuffix = context.getEnvironment().resolvePlaceholders(AZURE_STORAGE_ENDPOINT_SUFFIX);
        String connectionString = String.format(CONNECTION_STRING,storageAccountName,storageAccountKey,endpointSuffix);
        log.info("Azure storage connection string: {}", connectionString);
        return connectionString;
    }

    public String getPayloadFilename()
    {
        String fileName = context.getEnvironment().resolvePlaceholders(PAYLOAD_FILE_NAME);
        fileName = fileName.concat("_").concat(UUID.randomUUID().toString()).concat(".dat");
        log.info("Payload file name: {}", fileName);
        return fileName;
    }

}
