package com.example.centralstationkafka.services;

import com.example.centralstationkafka.bitcaskAndParquet.BaseCentralStation.ParquetFileWriter;
import com.example.centralstationkafka.bitcaskAndParquet.BaseCentralStation.StationMessage;
import com.example.centralstationkafka.bitcaskAndParquet.Bitcask;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.hadoop.ParquetWriter;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class KafkaListeners {
    ParquetFileWriter writer = new ParquetFileWriter();
    ObjectMapper mapper = new ObjectMapper();
    static Bitcask bitcask = new Bitcask();
    static {
        try {
            bitcask.open("/hello/bitcaskFile");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @KafkaListener(topics="weatherStation", groupId = "my-group-id")
    void listen(String data) throws Exception {
        System.out.println("hello   " + data);
        StationMessage message = mapper.readValue(data,StationMessage.class);
        writer.receive(message);
        byte key = (byte) message.getStation_id();
        byte[] keyBytes = new byte[1];
        keyBytes[0] = key;
        byte[] valueBytes = data.getBytes();

        bitcask.put(keyBytes,valueBytes);
    }
}
