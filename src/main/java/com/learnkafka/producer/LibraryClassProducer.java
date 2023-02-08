package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryClassProducer {

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    private final ObjectMapper mapper;

    public LibraryClassProducer(KafkaTemplate<Integer, String> kafkaTemplate,
                                ObjectMapper mapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
    }

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        var key = libraryEvent.getLibraryEventId();
        var value = mapper.writeValueAsString(libraryEvent);

        var completableFuture = kafkaTemplate.sendDefault(key, value);
        var sendResult = completableFuture.get();

        handleSuccess(key, value, sendResult);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message send Successfully for the key :{} and value is {}, partition is {}", key, value, sendResult.getRecordMetadata().partition());
    }
}
