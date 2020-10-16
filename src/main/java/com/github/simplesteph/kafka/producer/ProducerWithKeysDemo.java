package com.github.simplesteph.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeysDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerWithCallBackDemo.class);

        // Step1 : Create Producer Properties
        // --> you can get a list of config options in :
        // https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //because kafka client converts the messages to bytes and send to kafka we have to set these two props to
        // know how to serialize(convert to byte) them and in this case both are String so we use StringSerializer
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step2 : Create the Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<5; i++) {

            String topic = "first_topic";
            String key = "id_" + Integer.toString(i);
            String  value = "Hello " + key;

            // Step3 : Send Data - asynchronous
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key);
            // guarantee that same keys always go to same partition
            // id_0 is going to partition 1
            // id_1 is going to partition 0
            // id_2 is going to partition 2
            // id_3 is going to partition 0
            // id_4 is going to partition 2
            // with same number of partitions (here = 3) the result is exact same in every machine

            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exception thrown
                    if(e == null){
                        //the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    }else{
                        logger.error("Error while Producing ", e);
                    }
                }
            }).get(); //using get() make it synchronous - don't do this in production
        }

        // Flush data
        kafkaProducer.flush();

        // Flush and Close producer
        kafkaProducer.close();
    }
}
