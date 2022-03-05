package com.github.mellik21.simple.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.github.mellik21.util.ConstantKeeper.BOOTSTRAP_SERVERS;
import static com.github.mellik21.util.ConstantKeeper.TOPIC_NAME;

@Slf4j
public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = createProducer();

        for (int i = 0; i < 10; i++) {
            String value = "HEY HEY #" + i;
            String key = "id_" + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
            log.info("Let's start sending record with Key=" + key);

            //-asynchronous
            producer.send(record, getCallbackAndLog())
                    .get(); //block the .send to make it synchronous
        }
        producer.flush();
        producer.close();
    }

    private static KafkaProducer<String, String> createProducer() {
        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        return new KafkaProducer<>(properties);
    }

    private static Callback getCallbackAndLog() {
        return (recordMetadata, e) -> {
            if (e == null) {
                log.info("Recieved new metadata \n" +
                        "Topic:" + recordMetadata.topic() + "\n" +
                        "Partition:" + recordMetadata.partition() + "\n" +
                        "Offset:" + recordMetadata.offset() + "\n" +
                        "Timestamp:" + recordMetadata.timestamp());
            } else {
                log.error("Error while producing", e.getMessage());
            }
        };
    }

}
