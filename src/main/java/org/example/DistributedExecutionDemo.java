package org.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class DistributedExecutionDemo {

    // ZooKeeper connection string
    private static final String ZOOKEEPER_CONNECTION_STRING = "localhost:2181";
    // Session timeout
    private static final int SESSION_TIMEOUT = 3000;
    // Path for the distributed lock
    private static final String LOCK_PATH = "/distributed_lock";

    // ZooKeeper client
    private ZooKeeper zooKeeper;

    // Constructor
    public DistributedExecutionDemo() {
        try {
            // Initialize ZooKeeper client
            this.zooKeeper = new ZooKeeper(ZOOKEEPER_CONNECTION_STRING, SESSION_TIMEOUT, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to execute distributed task
    public void executeDistributedTask() {
        try {
            // Create a persistent node for the lock
            zooKeeper.create(LOCK_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            // Attempt to acquire the lock
            String lockPath = zooKeeper.create(LOCK_PATH + "/lock_", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            // Check if the created node is the smallest one
            while (true) {
                String minNode = getSmallestNode();
                if (lockPath.equals(minNode)) {
                    // You have acquired the lock
                    System.out.println("Lock acquired. Starting distributed task.");
                    break;
                } else {
                    // If not the smallest node, wait for the smallest node to be deleted
                    CountDownLatch latch = new CountDownLatch(1);
                    Stat stat = zooKeeper.exists(minNode, new Watcher() {
                        @Override
                        public void process(WatchedEvent event) {
                            if (event.getType() == Event.EventType.NodeDeleted) {
                                latch.countDown();
                            }
                        }
                    });
                    if (stat != null) {
                        latch.await();
                    }
                }
            }

            // Simulate the distributed task execution
            Thread.sleep(5000);

            // Release the lock
            zooKeeper.delete(lockPath, -1);
            System.out.println("Lock released. Distributed task completed.");

        } catch (KeeperException | InterruptedException  e) {
            e.printStackTrace();
        }
    }

    // Method to get the smallest node under the lock path
    private String getSmallestNode() throws KeeperException, InterruptedException {
        String minNode = null;
        int minSeq = Integer.MAX_VALUE;
        for (String node : zooKeeper.getChildren(LOCK_PATH, false)) {
            int seq = Integer.parseInt(node.substring(node.lastIndexOf("_") + 1));
            if (seq < minSeq) {
                minSeq = seq;
                minNode = node;
            }
        }
        return LOCK_PATH + "/" + minNode;
    }

    // Main method
    public static void main(String[] args) {
        // Create an instance of DistributedExecutionDemo
        DistributedExecutionDemo demo = new DistributedExecutionDemo();
        // Execute the distributed task
        demo.executeDistributedTask();
    }
}
