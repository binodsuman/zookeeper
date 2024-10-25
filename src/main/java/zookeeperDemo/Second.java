package zookeeperDemo;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Second {

    private static final Logger log = LoggerFactory.getLogger(Second.class);
    private static final String ZOOKEEPER_CONNECTION_STRING = "localhost:2181,localhost:2182,localhost:2183";
    private static final int SESSION_TIMEOUT = 3000;



    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        log.info("----------------------------------------");
        System.out.println("Connecting to zookeeper");
        new Second().experiment();
     }

     private void experiment() throws IOException, InterruptedException, KeeperException {
         log.info("----------------------------------------");
        // ZooKeeper zooKeeper = new ZooKeeper(ZOOKEEPER_CONNECTION_STRING,SESSION_TIMEOUT,null);
         ZooKeeper zooKeeper = new ZooKeeper(ZOOKEEPER_CONNECTION_STRING, SESSION_TIMEOUT, null);


               // Update Node
         zooKeeper.setData("/mydata","From Intellig itself".getBytes(), -1);

         // Delete
         //zooKeeper.delete("mydata/child",-1);
         //zooKeeper.delete("mydata",-1);

         //Thread.sleep(100000000);
     }
}


/**
 *
 *
 *
 *
 * import org.apache.zookeeper.*;
 * import org.apache.zookeeper.data.Stat;
 * import org.slf4j.Logger;
 * import org.slf4j.LoggerFactory;
 *
 * import java.io.IOException;
 * import java.util.List;
 *
 * import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;
 *
 * public class A_Simple {
 *     private static final Logger LOG = LoggerFactory.getLogger(A_Simple.class);
 *
 *     public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
 *         ZooKeeper zookeeper = new ZooKeeper("localhost:2181", 15000, new Watcher() {
 *             @Override
 *             public void process(WatchedEvent watchedEvent) {
 *                 LOG.info("*********************************************************");
 *                 LOG.info("got the event for node = "+ watchedEvent.getPath());
 *                 LOG.info("the event type = "+ watchedEvent.getType());
 *                 LOG.info("*********************************************************");
 *             }
 *         });
 *
 *         // CREATE
 *         zookeeper.create("/node", "data".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, null);
 *
 *         // READ
 *         Stat stat = new Stat();
 *         var data = zookeeper.getData("/node", true, stat);
 *
 *         Thread.sleep(100_000);
 *     }
 * }
 *
 *
 */