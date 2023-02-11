package com.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryClassProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @MockBean
    LibraryClassProducer libraryClassProducer;

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void postLibraryEvent() throws Exception {
        // given
        var book = Book.builder()
                .bookId(123)
                .bookAuthor("Dima")
                .bookName("Kafka using Spring Boot")
                .build();

        var libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        var json = objectMapper.writeValueAsString(libraryEvent);

        doNothing().when(libraryClassProducer).sendLibraryEvent_Approach2(isA(LibraryEvent.class));

        // when
        mockMvc.perform(post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());

        // then
    }

    @Test
    void postLibraryEvent_4xx() throws Exception {
        // given

        var book = Book.builder()
                .bookId(123)
                .bookAuthor("Dima")
                .bookName("Kafka using Spring Boot")
                .build();

        var libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(new Book())
                .build();

        var json = objectMapper.writeValueAsString(libraryEvent);

        doNothing().when(libraryClassProducer).sendLibraryEvent_Approach2(isA(LibraryEvent.class));

        // when
        mockMvc.perform(post("/v1/libraryevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError());

    }
}
