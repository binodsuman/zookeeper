package kafkaKeyPartition;

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

public class KafkaConsumerWithKey {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerWithKey.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // Your Kafka broker(s)
    private static final String TOPIC_NAME = "key_partition_demo_topic";
    private static final String CONSUMER_GROUP_ID = "CG_1";

    public static void main(String[] args) {
        // 1. Create Consumer Properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        // auto.offset.reset: What to do when there is no initial offset in Kafka or if the current offset does not exist
        // (e.g., because the data has been deleted).
        // "earliest": automatically reset the offset to the earliest offset.
        // "latest": automatically reset the offset to the latest offset.
        // "none": throw exception to the consumer if no previous offset is found for the consumer group.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 2. Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // Add a shutdown hook to handle graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown hook, exiting by calling consumer.wakeup()...");
            consumer.wakeup(); // Causes consumer.poll() to throw WakeupException
            try {
                mainThread.join(); // Wait for the main thread to finish
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for main thread to finish: {}", e.getMessage());
            }
        }));

        try {
            // 3. Subscribe consumer to our topic(s)
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            // 4. Poll for new data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // Poll every 100ms

                for (ConsumerRecord<String, String> record : records) {
                    log.info(CONSUMER_GROUP_ID+ " -> Consumed message -> Key: {}, Value: {}, Partition: {}, Offset: {}",
                            record.key(), record.value(), record.partition(), record.offset());
                    // Here you would add your business logic to process the message.
                    // Notice that all messages for a given key will have the same partition.
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is shutting down gracefully via WakeupException.");
        } catch (Exception e) {
            log.error("Error in consumer: {}", e.getMessage());
        } finally {
            consumer.close(); // Close the consumer, committing final offsets
            log.info("Consumer closed.");
        }
    }
}