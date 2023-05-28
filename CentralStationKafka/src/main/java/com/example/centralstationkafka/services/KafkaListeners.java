package com.example.centralstationkafka.services;

import com.example.centralstationkafka.bitcaskAndParquet.BaseCentralStation.ParquetFileWriter;
import com.example.centralstationkafka.bitcaskAndParquet.BaseCentralStation.StationMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.hadoop.ParquetWriter;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaListeners {
    @KafkaListener(topics="weatherStation", groupId = "my-group-id")
    void listen(String data) throws JsonProcessingException {

        System.out.println("hello   " + data);
//        ObjectMapper mapper = new ObjectMapper();
//        StationMessage message = mapper.readValue(data,StationMessage.class);
//        ParquetFileWriter writer = new ParquetFileWriter();
//        writer.receive(message);
    }
}
