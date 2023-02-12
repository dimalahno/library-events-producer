package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
@AllArgsConstructor
public class LibraryEventProducer {

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

        List<Header> recordsHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));

        return new ProducerRecord<>(topic, null, key, value, recordsHeaders);
    }

    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.getLibraryEventId();
        var value = mapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult;
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
