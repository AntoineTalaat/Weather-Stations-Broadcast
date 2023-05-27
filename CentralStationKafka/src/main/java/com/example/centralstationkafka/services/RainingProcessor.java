package com.example.centralstationkafka.services;

import com.example.centralstationkafka.model.RainDetectionMessage;
import com.google.gson.Gson;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.json.JSONException;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;

//@Component
public class RainingProcessor implements ProcessorSupplier<String, String, String, String> {

    final String storeName;
    final StoreBuilder<KeyValueStore<String, String>> messageStoreBuilder;
    public RainingProcessor(String storeName, StoreBuilder<KeyValueStore<String, String>> messageStoreBuilder)
    {
        this.storeName = storeName;
        this.messageStoreBuilder = messageStoreBuilder;
    }
    @Override
    public Set<StoreBuilder<?>> stores() {
        return Collections.singleton(messageStoreBuilder);
    }
    @Override
    public Processor<String, String, String, String> get() {
        return new Processor<>(){
            private ProcessorContext<String, String> context;
            private KeyValueStore<String, String> store;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.context = context;
                store = context.getStateStore(storeName);
                this.context.schedule(Duration.ofMillis(10), PunctuationType.STREAM_TIME, this::forwardAll);
            }
            private void forwardAll(final long timestamp) {
                try (KeyValueIterator<String, String> iterator = store.all()) {
                    while (iterator.hasNext()) {
                        final KeyValue<String, String> nextKV = iterator.next();
                        final Record<String, String> totalPriceRecord = new Record<>(nextKV.key, nextKV.value, timestamp);
                        context.forward(totalPriceRecord);
                    }
                }
            }

            @Override
            public void process(Record<String, String> record) {
                JSONObject jsonObject;
                try {
                    jsonObject = new JSONObject(record.value());
                } catch (JSONException e) {
                    throw new RuntimeException(e);
                }

                // Check if humidity is higher than 70%
                try {
                    if (jsonObject.getJSONObject("weather").getInt("humidity") > 70) {
                        // Forward the message to the rain.topic
//                        context.forward(record, To.child("rainingTriggers"));
//                        if(record.key() == null){
//                            System.out.println("lollllll");
//                            store.put("weatherStation", record.value());
//                        }
//                        store.put(record.key(), record.value());
                        long stationNo = jsonObject.getLong("station_id");
                        String warning =  "It is raining in " + stationNo;

                        RainDetectionMessage rainDetectionMessage = new RainDetectionMessage("weatherStation",
                                stationNo, warning, record.value());
                        store.put("weatherStation", new Gson().toJson(rainDetectionMessage));
                    }
                } catch (JSONException e) {
                    throw new RuntimeException(e);

                }
            }
//            @Override

            @Override
            public void close() {
                Processor.super.close();
            }
//            private con
        };
    }

    /*private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(String key, String value) {
        JSONObject jsonObject;
        try {
            jsonObject = new JSONObject(value);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }

        // Check if humidity is higher than 70%
        try {
            System.out.println("++++++++" + jsonObject);
            if (jsonObject.getJSONObject("weather").getInt("humidity") > 70) {
                // Forward the message to the rain.topic
                jsonObject.getJSONObject("weather").put("temperature", "a7eeeeeeeeh");
                context.forward(key, new Gson().toJson(jsonObject));
            }
        } catch (JSONException e) {
//            throw new RuntimeException(e);
            System.out.println("Message Dropped");
        }

        // Commit the current processing progress
        context.commit();
    }

    @Override
    public void close() {

    }*/

//    public static void main(String[] args) {
//        Properties config = new Properties();
//        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "weatherStation");
//        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//
//        StreamsBuilder builder = new StreamsBuilder();
//
//        KStream<String, String> input = builder.stream("input-topic");
//        KStream<String, String> output = builder.stream("output-topic");
//
//
//    }
}






/**
public class  RainingProcessor implements Processor<String, String> {

    private ProcessorContext context;

    @Value("${rain.topic}")
    private String rainTopic;

    @Autowired
    private KafkaTemplate<String, RainDetectionMessage> kafkaTemplate;

//    @Override
//    public void init(ProcessorContext context) {
//        this.context = context;
//    }

    /*@Override
    public void process(String key, String value) {
        JSONObject jsonObject;
        try {
            jsonObject = new JSONObject(value);

            // Check if humidity is higher than 70%
            if (jsonObject.getJSONObject("weather").getInt("humidity") > 70) {
                Long stationId = jsonObject.getLong("station_id");
                // Create a special message for rain detection
                RainDetectionMessage rainDetectionMessage = new RainDetectionMessage(key, stationId,
                        "It is raining at " + stationId + " nearby Places", value);

                // Forward the special message to the rain topic
                context.forward(key, rainDetectionMessage);
            }
        }
        catch (JSONException jsonException)
        {
            System.out.println("Message Dropped");
        }
    }*/
/**
//    @KafkaListener(topics = "weatherStation")
//    public void listen(String key, String value) {
//        // Process incoming messages from the weatherStation topic
//        process(key, value);
//    }

    @Override
    public void process(Record record) {

    }

    @Override
    public void init(org.apache.kafka.streams.processor.api.ProcessorContext context) {
        Processor.super.init(context);
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}**/