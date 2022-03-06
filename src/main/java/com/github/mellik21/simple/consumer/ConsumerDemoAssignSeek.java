package com.github.mellik21.simple.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.github.mellik21.util.ConstantKeeper.*;

@Slf4j
public class ConsumerDemoAssignSeek {

    private static final Long OFFSET_TO_READ_FROM = 15L;
    private static final int NUMBER_OF_MESSAGES_TO_READ = 5;

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createConsumer();

        //assign and seek are mostly used to replay data or fetch a specific message
        TopicPartition partitionToReadFrom = new TopicPartition(TOPIC_NAME, 0);
        consumer.assign(Collections.singleton(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, OFFSET_TO_READ_FROM);

        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        //poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(HUNDRED_MILLISECOND));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar += 1;
                log.info("Key:" + record.key() + "\n" +
                        "Value:" + record.value() + "\n" +
                        "Topic:" + record.topic() + "\n" +
                        "Partition:" + record.partition() + "\n" +
                        "Offset:" + record.offset() + "\n" +
                        "Timestamp:" + record.timestamp());
                if (numberOfMessagesReadSoFar >= NUMBER_OF_MESSAGES_TO_READ) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        log.info("Exiting the application");
    }

    private static KafkaConsumer<String, String> createConsumer() {
        //create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create the consumer
        return new KafkaConsumer<>(properties);
    }

}
