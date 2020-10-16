package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThreadDemo {

    private final Logger logger = LoggerFactory.getLogger(ConsumerWithThreadDemo.class);

    public static void main(String[] args) {
        new ConsumerWithThreadDemo().run();
    }

    private ConsumerWithThreadDemo(){
    }

    private void run() {

        //create the latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //create the consumer runnable
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(latch, "first_topic");

        // start the thread
        Thread myConsumerThread = new Thread(myConsumerRunnable);
        myConsumerThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interrupted ", e);
        }finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerRunnable(CountDownLatch latch, String topic){
            this.latch = latch;

            // Step1 : Create Consumer Properties
            // --> you can get a list of config options in :
            // https://kafka.apache.org/documentation/#consumerconfigs
            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-second-app");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//means read from the beginning of topic --> "latest" means read just new messages


            // Step2 : Create consumer
            kafkaConsumer = new KafkaConsumer<>(properties);

            // Step3 : Subscribe consumer to our topic(s)
            kafkaConsumer.subscribe(Collections.singleton(topic));
            // subscribe to multiple topics ex: kafkaConsumer.subscribe(Arrays.asList("first_topic","second_topic));

        }

        @Override
        public void run() {

            try {
                // Step4 : Poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + " Value: " + record.value());
                        logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("Received Shutdown Signal!");
            }finally {
                kafkaConsumer.close();
                // tell our main code we're done with the consumer
                latch.countDown();
            }

        }

        public void shutdown() {
            // wakeup() use to interrupt consumer.poll()
            // it will throw WakeUpException
            kafkaConsumer.wakeup();
        }
    }
}
