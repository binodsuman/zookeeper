package kafkaDemo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Kafka_Resource_Sharing {

private static final String BOOTSTRAP_SERVERS = "localhost:9092";
private static final String GROUP_ID = "distributed";
private static final String TOPIC = "sharing";
    Properties props = null;


    public static void main(String[] args) throws InterruptedException {
        Kafka_Resource_Sharing demo = new Kafka_Resource_Sharing();
        demo.basicSetup();
    }

public void basicSetup(){

        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        UUID uuid = UUID.randomUUID();
        String id =  UUID.randomUUID().toString();

        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "worker-"+id);
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));


        // Notification from Kafka Broker
        ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions)
            { }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                try {
                    callBackMethod(props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG),partitions);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        };

         consumer.subscribe(Collections.singletonList(TOPIC), rebalanceListener);

         while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            }
    }

        private  void callBackMethod(String consumer_id,Collection<TopicPartition> partitions) throws InterruptedException, ExecutionException {
        // Implement your logic here when a new consumer is added
            System.out.println("Number of Tasks :"+partitions.size());
             if (partitions.size() > 0) {
                int minPartition = Integer.MAX_VALUE;
                int maxPartition = Integer.MIN_VALUE;

                for (TopicPartition partition : partitions) {
                    //System.out.println("Partition details : "+partition);
                    minPartition = Math.min(minPartition, partition.partition());
                    maxPartition = Math.max(maxPartition, partition.partition());
                }
                System.out.println("Consumer ID : "+consumer_id+" Partition range: " + minPartition + "-" + maxPartition+" \n\n");
             }
      }
  }
