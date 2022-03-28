package com.maersk.kafkautility.service;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public interface AzureBlobService <T extends Serializable> {

    String writePayloadToBlob(T payload) throws URISyntaxException, InvalidKeyException, StorageException, IOException;

    ProducerRecord<String, T> storePayloadToBlob(String topic, T payload) throws URISyntaxException, InvalidKeyException, StorageException, IOException;

    T readPayloadFromBlob(String blobReference) throws URISyntaxException, InvalidKeyException, StorageException;

    T getPayloadFromBlob(T payloadReference, String isLargePayload) throws URISyntaxException, InvalidKeyException, StorageException;

    void deletePayloadFromBlob(String blobReference) throws URISyntaxException, InvalidKeyException, StorageException;

    URI writePayloadFileToBlob(T payload, CloudBlobContainer containerDest) throws URISyntaxException, StorageException, IOException;
}
