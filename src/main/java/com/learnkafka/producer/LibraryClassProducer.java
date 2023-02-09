package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Component
@Slf4j
@AllArgsConstructor
public class LibraryClassProducer {

    private final String TOPIC = "library-events";

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    private final ObjectMapper mapper;

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        var key = libraryEvent.getLibraryEventId();
        var value = mapper.writeValueAsString(libraryEvent);

        var completableFuture = kafkaTemplate.sendDefault(key, value);
        var sendResult = completableFuture.get();

        handleSuccess(key, value, sendResult);
    }

    public void sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        var key = libraryEvent.getLibraryEventId();
        var value = mapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, TOPIC);
        var completableFuture = kafkaTemplate.send(producerRecord);
        var sendResult = completableFuture.get();

        handleSuccess(key, value, sendResult);
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        return new ProducerRecord<>(topic, null, key, value);
    }

    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.getLibraryEventId();
        var value = mapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult = null;
        try {
            sendResult = kafkaTemplate.sendDefault(key, value).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error Sending the Message and the exception is {}" ,e.getMessage());
            throw new RuntimeException(e);
        }

        return sendResult;
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message send Successfully for the key :{} and value is {}, partition is {}", key, value, sendResult.getRecordMetadata().partition());
    }
}
