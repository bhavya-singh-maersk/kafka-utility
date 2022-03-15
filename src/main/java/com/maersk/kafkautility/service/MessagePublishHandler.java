package com.maersk.kafkautility.service;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@Component
@NoArgsConstructor
@AllArgsConstructor
public class MessagePublishHandler <T>{

    @Autowired
    private KafkaTemplate<String, T> kafkaTemplate;

    public void publishOnTopic(ProducerRecord<String, T> producerRecord)
    {
        log.info("Inside publishOnTopic");
        ListenableFuture<SendResult<String, T>> future = kafkaTemplate.send(producerRecord);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, T> result) {
                log.info("Sent message=[{}] with offset=[{}]", producerRecord.value(), result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("Unable to send message=[{}] due to : {}", producerRecord.value(), ex);
            }
        });
        log.info("Completed publishOnTopic");
    }
}
