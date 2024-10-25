package kafkaDemo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class KafkaConsumerPartitionNotification {

private static final String BOOTSTRAP_SERVERS = "localhost:9092";
private static final String GROUP_ID = "distribution";
private static final String TOPIC = "dist";


public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        UUID uuid = UUID.randomUUID();
        String id =  UUID.randomUUID().toString();

        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer - "+id);
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

 ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions)
        {
                System.out.println("onPartitionsRevoked **");
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // Handle partition assignment (e.g., log, initialize)
        // Call your method when new consumer is added
                if (partitions.size() > 0) {
                        // Multiple consumers in the group
                        System.out.println("Number of  consumer chnaged group :"+partitions.size());

                        callYourMethod();
                }



            int minPartition = Integer.MAX_VALUE;
            int maxPartition = Integer.MIN_VALUE;

            for (TopicPartition partition : partitions) {
                System.out.println("Partition details : "+partition);
                minPartition = Math.min(minPartition, partition.partition());
                maxPartition = Math.max(maxPartition, partition.partition());
            }
            String consumerId = props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
            System.out.println("Consumer ID : "+consumerId+" Partition range: " + minPartition + "-" + maxPartition);

        }

        };

        consumer.subscribe(Collections.singletonList(TOPIC), rebalanceListener);


        while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                        // Process the record  
                        System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                }
         }
        }

        private static void callYourMethod() {
        // Implement your logic here when a new consumer is added
                System.out.println("Calling your method...");
                }

}
