package com.github.mellik21.simple.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.github.mellik21.util.ConstantKeeper.*;

@Slf4j
public class ConsumerDemoGroups {
    private static final String ANOTHER_GROUP_ID = "another-group-id";

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createConsumer();

        //subscribe consumer to our topic(s)
        consumer.subscribe(Collections.singleton(TOPIC_NAME));

        //poll for new data
        while (true) {
            // consumer.poll(HUNDRED_MILLISECOND); //deprecated from kafka 2.0.0
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
    }

    private static KafkaConsumer<String, String> createConsumer() {
        //create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ANOTHER_GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create the consumer
        return new KafkaConsumer<>(properties);
    }

}
