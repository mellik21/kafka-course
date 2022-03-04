package com.github.mellik21.simple.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static com.github.mellik21.util.ConstantKeeper.BOOTSTRAP_SERVERS;
import static com.github.mellik21.util.ConstantKeeper.TOPIC_NAME;

@Slf4j
public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, "Hello world");

        //send data
        producer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                log.info("Recieved new metadata \n" +
                        "Topic:" + recordMetadata.topic() + "\n" +
                        "Partition:" + recordMetadata.partition() + "\n" +
                        "Offset:" + recordMetadata.offset() + "\n" +
                        "Timestamp:" + recordMetadata.timestamp());
            } else {
                log.error("Error while producing", e);
            }

        });
        producer.close();
    }
}
