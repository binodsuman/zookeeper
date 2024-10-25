package org.example;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class DistributedPrintingDemo_2 {

    private static final String ZOOKEEPER_CONNECTION_STRING = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String LOCK_PATH = "/printing_lock2";
    private static final String SEQUENCE_PATH = "/printing_sequence";
    private static final int TOTAL_NUMBERS = 10000;

    private ZooKeeper zooKeeper;

    public DistributedPrintingDemo_2() {
        try {
            this.zooKeeper = new ZooKeeper(ZOOKEEPER_CONNECTION_STRING, SESSION_TIMEOUT, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void printNumbers() {
        try {
            // Create the lock node
            //zooKeeper.create(LOCK_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            for (int i = 1; i <= TOTAL_NUMBERS; i++) {
                // Acquire lock
                String lockPath = zooKeeper.create(LOCK_PATH + "/lock_", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

                // Wait until it becomes the leader
                waitForLeader(lockPath);

                // Print the number
                System.out.println("Machine " + Thread.currentThread().getId() + " prints: " + i);

                // Release lock
                zooKeeper.delete(lockPath, -1);
            }

        } catch (KeeperException | InterruptedException  e) {
            e.printStackTrace();
        }
    }

    private void waitForLeader(String lockPath) throws KeeperException, InterruptedException {
        while (true) {
            int currentSeq = getSequenceNumber(lockPath);
            if (isLeader(lockPath, currentSeq)) {
                break;
            } else {
                waitForChange(lockPath, currentSeq);
            }
        }
    }

    private int getSequenceNumber(String lockPath) throws KeeperException, InterruptedException {
        String[] parts = lockPath.split("/");
        String sequenceNode = parts[parts.length - 1];
        return Integer.parseInt(sequenceNode.substring(sequenceNode.lastIndexOf("_") + 1));
    }

    private boolean isLeader(String lockPath, int currentSeq) throws KeeperException, InterruptedException {
        String[] children = zooKeeper.getChildren(LOCK_PATH, false).toArray(new String[0]);
        int minSeq = Integer.MAX_VALUE;
        for (String child : children) {
            int seq = getSequenceNumber(LOCK_PATH + "/" + child);
            if (seq < minSeq) {
                minSeq = seq;
            }
        }
        return currentSeq == minSeq;
    }

    private void waitForChange(String lockPath, int currentSeq) throws KeeperException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        String previousNode = LOCK_PATH + "/lock_" + String.format("%010d", currentSeq - 1);
        zooKeeper.exists(previousNode, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeDeleted) {
                    latch.countDown();
                }
            }
        });
        latch.await();
    }

    public static void main(String[] args) {
        DistributedPrintingDemo_2 demo = new DistributedPrintingDemo_2();
        demo.printNumbers();
    }
}
