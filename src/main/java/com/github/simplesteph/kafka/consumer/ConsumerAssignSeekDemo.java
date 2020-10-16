package com.github.simplesteph.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAssignSeekDemo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        // Step1 : Create Consumer Properties
        // --> you can get a list of config options in :
        // https://kafka.apache.org/documentation/#consumerconfigs
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//means read from the beginning of topic --> "latest" means read just new messages

        // Step2 : Create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // Step3 : Assign and Seeak

        // assign and seek are mostly used to replay data or fetch a specific message

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition("first_topic", 2);
        kafkaConsumer.assign(Arrays.asList(partitionToReadFrom));

        // seek --> means to go to a specific offset and read message from there
        long offsetToReadFrom = 5L;
        kafkaConsumer.seek(partitionToReadFrom, offsetToReadFrom);

        // Step4 : Poll for new data
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;

        while (keepOnReading){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records){
                logger.info("Key: " + record.key() + " Value: " + record.value());
                logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                if(--numberOfMessagesToRead == 0) keepOnReading = false;
            }
        }
    }

}
