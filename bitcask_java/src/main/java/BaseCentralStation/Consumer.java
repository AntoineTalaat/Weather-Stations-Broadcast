package BaseCentralStation;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

class Consumer {

//    public static void main(String[] args) {
//        new Consumer(Configs.server, "12", Configs.topic).run();
//    }

    private final Logger mLogger = LoggerFactory.getLogger(Consumer.class.getName());
    private final String mBootstrapServer;
    private final String mGroupId;
    private final String mTopic;

    Consumer(String bootstrapServer, String groupId, String topic) {
        mBootstrapServer = bootstrapServer;
        mGroupId = groupId;
        mTopic = topic;
    }

    void run() {
        mLogger.info("Creating consumer thread");

        CountDownLatch latch = new CountDownLatch(1);

        ConsumerRunnable consumerRunnable = new ConsumerRunnable(mBootstrapServer, mGroupId, mTopic, latch);
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            mLogger.info("Caught shutdown hook");
            consumerRunnable.shutdown();
            await(latch);

            mLogger.info("Application has exited");
        }));

        await(latch);
    }

    void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            mLogger.error("Application got interrupted", e);
        } finally {
            mLogger.info("Application is closing");
        }
    }

    private class ConsumerRunnable implements Runnable {

        private CountDownLatch mLatch;
        private KafkaConsumer<String, String> mConsumer;

        ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {
            mLatch = latch;

            Properties props = consumerProps(bootstrapServer, groupId);
            mConsumer = new KafkaConsumer<>(props);
            mConsumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    //fetch records every 100 millis
                    ConsumerRecords<String, String> records = mConsumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {


                        mLogger.info("Key: " + record.key() + " Partition: " + record.partition() + ", Offset: " + record.offset());
//                        File myObj = new File("/home/mohamed/Desktop/kafka/kafka/ConsumerData/"+ record.key() +".txt");
//                        FileWriter writer = new FileWriter(myObj) ;
//                        writer.write(record.value());
//                        writer.flush();
//                        writer.close();

                        if(Configs.messageLatency.size() == Configs.messages){
                            Collections.sort(Configs.messageLatency);
                            mLogger.info("Median Latency = " + Configs.messageLatency.get(Configs.messages/2) + "\n");
                        }
                        Configs.consumerMessages += 1 ;
                        if(Configs.consumerMessages == Configs.messages){
                            System.out.println(Configs.consumerResponseTime / Configs.messages);
                        }
                    }
                }
            } catch (WakeupException E) {
                mLogger.info("Received shutdown signal!");
            } finally {
                mConsumer.close();
                mLatch.countDown();
            }
        }

        void shutdown() {
            mConsumer.wakeup();
        }

        private Properties consumerProps(String bootstrapServer, String groupId) {
            String deserializer = StringDeserializer.class.getName();
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            return properties;
        }
    }
}