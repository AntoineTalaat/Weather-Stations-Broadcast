package com.example.centralstationkafka.services;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaSpecialMessageListeners {
    @KafkaListener(topics="rainingTriggers")
    void listen(String data){
        System.out.println("hi " + data);
    }
}
