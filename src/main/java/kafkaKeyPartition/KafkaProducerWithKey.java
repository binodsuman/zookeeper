package kafkaKeyPartition;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerWithKey {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerWithKey.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // Your Kafka broker(s)
    private static final String TOPIC_NAME = "key_partition_demo_topic";

    public static void main(String[] args) {
        // 1. Create Producer Properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Optional: Acks for message durability. 'all' means leader and all in-sync replicas must acknowledge.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Optional: Retries to resend messages if transient errors occur
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // Optional: Idempotent producer ensures no duplicate messages on retries
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // 2. Specify the custom partitioner class.
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());


        // 2. Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            // Send messages for "user_A" - these should all go to the same partition
            for (int i = 0; i < 5; i++) {
                String key = "user_A";
                String value = "Message " + i + " for " + key;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Produced message to partition {} with offset {}. Key: {}, Value: {}",
                                metadata.partition(), metadata.offset(), key, value);
                    } else {
                        log.error("Error producing message for key {}: {}", key, exception.getMessage());
                    }
                }).get(); // .get() makes the send synchronous for demo purposes. Remove for async in production.
            }

            // Send messages for "user_B" - these should also all go to the same partition (different from user_A's)
            for (int i = 0; i < 5; i++) {
                String key = "user_B";
                String value = "Message " + i + " for " + key;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Produced message to partition {} with offset {}. Key: {}, Value: {}",
                                metadata.partition(), metadata.offset(), key, value);
                    } else {
                        log.error("Error producing message for key {}: {}", key, exception.getMessage());
                    }
                }).get();
            }

            // Send messages for "user_C"
            for (int i = 0; i < 5; i++) {
                String key = "user_C";
                String value = "Message " + i + " for " + key;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key,  value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Produced message to partition {} with offset {}. Key: {}, Value: {}",
                                metadata.partition(), metadata.offset(), key, value);
                    } else {
                        log.error("Error producing message for key {}: {}", key, exception.getMessage());
                    }
                }).get();
            }

            // Send messages for "user_D"
            /*for (int i = 0; i < 5; i++) {
                String key = "user_D";
                String value = "Message " + i + " for " + key;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME,   value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Produced message to partition {} with offset {}. Key: {}, Value: {}",
                                metadata.partition(), metadata.offset(), key, value);
                    } else {
                        log.error("Error producing message for key {}: {}", key, exception.getMessage());
                    }
                }).get();
            }*/

        } catch (InterruptedException | ExecutionException e) {
            log.error("Error during message production: {}", e.getMessage());
        } finally {
            // 3. Flush and Close the Producer
            producer.flush(); // Ensures all buffered records are sent
            producer.close(); // Closes the producer and flushes any outstanding records
            log.info("Producer closed.");
        }
    }
}