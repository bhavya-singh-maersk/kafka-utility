package com.maersk.kafkautility.service;

import com.maersk.kafkautility.utils.AzureUtil;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.openjdk.jol.vm.VM;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.util.Objects;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AzureBlobServiceImpl<T extends Serializable> implements AzureBlobService<T> {

    @Autowired
    private ApplicationContext context;

    private static final String PAYLOAD_SIZE = "${events-payload.max-bytes}";
    private static final String PAYLOAD_FILE_NAME = "${events-payload.file-name}";
    private static final String AZURE_STORAGE_ACCOUNT_NAME = "${azure.storage.account-name}";
    private static final String AZURE_STORAGE_ACCOUNT_KEY = "${azure.storage.account-key}";
    private static final String AZURE_STORAGE_CONTAINER_NAME = "${azure.storage.container-name}";
    private static final String AZURE_STORAGE_ENDPOINT_SUFFIX = "${azure.storage.endpoint-suffix}";
    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=%s";


    @Override
    public String writePayloadToBlob(T payload) throws URISyntaxException, InvalidKeyException, StorageException, IOException {
        var containerDest = getCloudBlobContainer();
        var blobUri = writePayloadFileToBlob(payload, containerDest);
        return blobUri.toString();
    }

    @Override
    public ProducerRecord<String, T> storePayloadToBlob(String topic, T payload) throws URISyntaxException, InvalidKeyException, StorageException, IOException {
        ProducerRecord<String, T> producerRecord = null;
        if (isLargePayload(payload))
        {
            log.info("Payload exceeds max configured size");
            var containerDest = getCloudBlobContainer();
            var blobUri = writePayloadFileToBlob(payload, containerDest);
            producerRecord = new ProducerRecord<>(topic, (T)blobUri.toString());
            producerRecord.headers().add("isLargePayload", "YES".getBytes(StandardCharsets.UTF_8));
        }
        else
        {
            producerRecord = new ProducerRecord<>(topic, payload);
            producerRecord.headers().add("isLargePayload", "NO".getBytes(StandardCharsets.UTF_8));
        }
        return producerRecord;
    }

    @Override
    public T readPayloadFromBlob(String blobReference) throws URISyntaxException, InvalidKeyException, StorageException {
        var containerDest = getCloudBlobContainer();
        T payload = readPayloadFileFromBlob(new URI(blobReference), containerDest);
        if (Objects.nonNull(payload))
        {
            Class<?> clazz = payload.getClass();
            log.info("Payload class name : {}", clazz.getName());
        }
        return payload;
    }

    @Override
    public T getPayloadFromBlob(T payloadReference, String isLargePayload) throws URISyntaxException, InvalidKeyException, StorageException {
        if (isLargePayload.equals("NO"))
        {
            return payloadReference;
        }
        var containerDest = getCloudBlobContainer();
        return readPayloadFileFromBlob(new URI(payloadReference.toString()), containerDest);
    }

    @Override
    public void deletePayloadFromBlob(String blobReference) throws URISyntaxException, InvalidKeyException, StorageException {
        var containerDest = getCloudBlobContainer();
        CloudBlockBlob cloudBlockBlob = containerDest.getBlockBlobReference(new CloudBlockBlob(new URI(blobReference)).getName());
        boolean deleted = cloudBlockBlob.deleteIfExists();
        log.info("Payload file deleted: {}", deleted);
    }

    private boolean isLargePayload(T payload)
    {
        var maxSize = context.getEnvironment().resolvePlaceholders(PAYLOAD_SIZE);
        var payloadSize = VM.current().sizeOf(payload);
        log.info("Payload size: {} bytes", payloadSize);
        return payloadSize > Long.parseLong(maxSize);
    }

    @Override
    public URI writePayloadFileToBlob(T payload, CloudBlobContainer containerDest) throws URISyntaxException, StorageException, IOException {
        CloudBlockBlob cloudBlockBlob = containerDest.getBlockBlobReference(getPayloadFilename());
        try (BlobOutputStream bos = cloudBlockBlob.openOutputStream()) {
            byte[] byteArray = SerializationUtils.serialize(payload);
            bos.write(byteArray);
        }
        return cloudBlockBlob.getUri();
    }

    private T readPayloadFileFromBlob(URI uri, CloudBlobContainer containerDest) throws StorageException, URISyntaxException {
        T payload = null;
        CloudBlockBlob cloudBlockBlob = containerDest.getBlockBlobReference(new CloudBlockBlob(uri).getName());
        try (BlobInputStream bis = cloudBlockBlob.openInputStream())
        {
            byte[] byteArray = bis.readAllBytes();
            payload = SerializationUtils.deserialize(byteArray);
            log.info("Payload after deserialization: {}", payload);
        } catch (IOException io) {
            log.error("Exception while reading payload from blob", io);
        }
        return payload;
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
