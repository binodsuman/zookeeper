package kafkaDemo;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

//Create java class named “SimpleProducer”
public class ProducerDemo {

    public static void main(String[] args) throws Exception{

        // Check arguments length value
        /*if(args.length == 0){
            System.out.println("Enter topic name");
            return;
        }*/

        //Assign topicName to string variable
        String topicName = "dist"; //args[0].toString();

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        for(int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));
            System.out.println(i);
            Thread.sleep(1000);
        }

        System.out.println("Message sent successfully");
        producer.close();
    }
}
