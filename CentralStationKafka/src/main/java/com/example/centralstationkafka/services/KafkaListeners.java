package com.example.centralstationkafka.services;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaListeners {
    @KafkaListener(topics="weatherStation", groupId = "my-group-id")
    void listen(String data){
        //TODO
        // TONY PARQUET
        System.out.println("hello   " + data);
    }
}
