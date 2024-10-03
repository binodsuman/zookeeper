package youtube.queue;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import youtube.leader.Leader_Selection;

public class Producer {

    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);
    private static final String ZOOKEEPER_SERVER = "localhost:2181";
    private static final int SESSION_TIMEOUT = 2000;

    private static final String ZNODE_NAMESPACE = "/queue_1";

    public static void main(String[] args) throws Exception {


        // Connect to the ZooKeeper instance running on localhost:2181
        ZooKeeper zooKeeper = new ZooKeeper(ZOOKEEPER_SERVER, SESSION_TIMEOUT, null);

        // Create a distributed queue and enqueue items

        if(zooKeeper.exists(ZNODE_NAMESPACE, false) ==  null){
            zooKeeper.create(ZNODE_NAMESPACE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        //NoLockQueue queue = new NoLockQueue(zooKeeper, ZNODE_NAMESPACE);
        int i = 0;
        while (i < Integer.MAX_VALUE){
            System.out.println("going to produce item"+i);
            String producerData = "item"+i;

            byte[] data = producerData.getBytes();
            zooKeeper.create(ZNODE_NAMESPACE + "/item", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);


            Thread.sleep(1000);
            i++;
        }

        // Close the ZooKeeper connection
        zooKeeper.close();
    }
}