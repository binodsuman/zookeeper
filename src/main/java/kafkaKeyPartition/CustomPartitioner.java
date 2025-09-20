package kafkaKeyPartition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // Get the number of available partitions for the topic.
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        // Check the key to determine the partition.
        if (key != null && key.toString().equals("user_A")) {
            // Send this key to a specific partition, e.g., partition 0.
            return 2;
        } else {
            // For all other keys, send them to a different partition, e.g., partition 1.
            return 1 % numPartitions; // Use modulo to ensure it's a valid partition number.
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No specific configuration needed in this simple example.
    }

    @Override
    public void close() {
        // No resources to close in this simple example.
    }
}