package com.example.centralstationkafka.services;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class Initilizer implements CommandLineRunner {
    @Value("${spring.kafka.bootstrap-servers}")
    private String boostrapServers;
    final static String storeName = "special-message-store";
    static StoreBuilder<KeyValueStore<String, String>> messageStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(storeName),
            Serdes.String(),
            Serdes.String());
    @Override
    public void run(String... args) throws Exception {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.boostrapServers);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        final String inputTopic = "weatherStation";
        final String outputTopic = "rainingTriggers";
        final Serde<String> uSerde = Serdes.String();
        final Serde<String> vSerde = Serdes.String();
        final Topology topology = new Topology();
        topology.addSource(
                "source-node",
                uSerde.deserializer(),
                vSerde.deserializer(),
                inputTopic);
        topology.addProcessor(
                "processor",
                new RainingProcessor(storeName, messageStoreBuilder),
                "source-node");
        topology.addSink(
                "sink-node",
                outputTopic,
                uSerde.serializer(),
                vSerde.serializer(),
                "processor");
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();
    }
}
