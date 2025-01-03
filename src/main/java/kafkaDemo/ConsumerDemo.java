package kafkaDemo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class ConsumerDemo {
    public static void main(String[] args) throws Exception {
//        if(args.length == 0){
//            System.out.println("Enter topic name");
//            return;
//        }
        //Kafka consumer configuration settings
        String topicName = "test"; // args[0].toString();
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "distribution");
       // props.put("enable.auto.commit", "true");
       // props.put("auto.commit.interval.ms", "1000");
       // props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.GROUP_ID_CONFIG, "basic-consumer");
       // props.put("group.id","ishan");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //Kafka Consumer subscribes list of topics here.
        List<String> allTopics = new ArrayList<>();
        allTopics.add("dist");
        //allTopics.add("test2");
        //consumer.subscribe(Arrays.asList(topicName));
        consumer.subscribe(allTopics);

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);
        int i = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)

                // print the offset,key and value for the consumer records.
                System.out.printf("From Topic -> %s, offset = %d, key = %s, value = %s\n",
                        record.topic(),record.offset(), record.key(), record.value());
        }
    }
}