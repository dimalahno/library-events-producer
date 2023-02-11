package com.learnkafka.domain;

import lombok.*;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class LibraryEvent {

    private Integer libraryEventId;
    private LibraryEventType type;
    @NonNull
    private Book book;

}
