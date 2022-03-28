package com.maersk.kafkautility.service;

import com.maersk.kafkautility.utils.AzureUtil;
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

@Slf4j
@Service
@RequiredArgsConstructor
public class AzureBlobServiceImpl<T extends Serializable> implements AzureBlobService<T> {

    @Autowired
    private ApplicationContext context;

    private static final String PAYLOAD_SIZE = "${events-payload.max-bytes}";

    @Override
    public String writePayloadToBlob(T payload) throws URISyntaxException, InvalidKeyException, StorageException, IOException {
        var containerDest = AzureUtil.getCloudBlobContainer();
        var blobUri = writePayloadFileToBlob(payload, containerDest);
        return blobUri.toString();
    }

    @Override
    public ProducerRecord<String, T> storePayloadToBlob(String topic, T payload) throws URISyntaxException, InvalidKeyException, StorageException, IOException {
        ProducerRecord<String, T> producerRecord = null;
        if (isLargePayload(payload))
        {
            log.info("Payload exceeds max configured size");
            var containerDest = AzureUtil.getCloudBlobContainer();
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
        var containerDest = AzureUtil.getCloudBlobContainer();
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
        var containerDest = AzureUtil.getCloudBlobContainer();
        return readPayloadFileFromBlob(new URI(payloadReference.toString()), containerDest);
    }

    @Override
    public void deletePayloadFromBlob(String blobReference) throws URISyntaxException, InvalidKeyException, StorageException {
        var containerDest = AzureUtil.getCloudBlobContainer();
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
        CloudBlockBlob cloudBlockBlob = containerDest.getBlockBlobReference(AzureUtil.getPayloadFilename());
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


}
