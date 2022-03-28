package com.maersk.kafkautility.service;

import com.microsoft.azure.storage.StorageException;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public interface KafkaProducerService <T> {

    ProducerRecord<String, String> getProducerRecord(T payload) throws URISyntaxException, IOException, InvalidKeyException, StorageException;

    ProducerRecord<String, T> readProducerRecord(T payload) throws URISyntaxException, IOException, InvalidKeyException, StorageException;
}
