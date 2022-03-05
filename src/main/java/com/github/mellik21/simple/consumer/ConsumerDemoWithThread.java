package com.github.mellik21.simple.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.github.mellik21.util.ConstantKeeper.*;

@Slf4j
public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private static KafkaConsumer<String, String> createConsumer() {
        //create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create the consumer
        return new KafkaConsumer<>(properties);
    }

    private ConsumerDemoWithThread() {

    }

    private void run() {
        log.info("Creating the consumer thead");
        CountDownLatch latch = new CountDownLatch(1);

        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(latch);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application got interrupted", e);
        } finally {
            log.info("Application is closing");
        }

    }

    public static class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer = createConsumer();

        public ConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;
            consumer.subscribe(Collections.singleton(TOPIC_NAME));
        }

        @Override
        public void run() {
            //poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(HUNDRED_MILLISECOND));

                    for (ConsumerRecord<String, String> record : records) {
                        log.info("Key:" + record.key() + "\n" +
                                "Value:" + record.value() + "\n" +
                                "Topic:" + record.topic() + "\n" +
                                "Partition:" + record.partition() + "\n" +
                                "Offset:" + record.offset() + "\n" +
                                "Timestamp:" + record.timestamp());
                    }
                }
            } catch (WakeupException e) {
                log.info("Recieved shutdown signal!");
            } finally {
                consumer.close();
                //tell main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            //special method to interrupt .poll
            //it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
