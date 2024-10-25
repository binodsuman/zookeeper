package zookeeperDemo.queue;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;


public class Consumer {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
    private static final String ZOOKEEPER_SERVER = "localhost:2181";
    private static final int SESSION_TIMEOUT = 20000000;

    private static final String ZNODE_NAMESPACE = "/queue_1";
    private static final String LOCK_NODE = "/locks_1";

    public static void main(String[] args) throws Exception {
        // Connect to the ZooKeeper instance running on localhost:2181
        ZooKeeper zooKeeper = new ZooKeeper(ZOOKEEPER_SERVER, SESSION_TIMEOUT, null);


       if(zooKeeper.exists(ZNODE_NAMESPACE, false) ==  null){
            zooKeeper.create(ZNODE_NAMESPACE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        if(zooKeeper.exists(LOCK_NODE, false) ==  null){
            zooKeeper.create(LOCK_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }




        /*for(int i=1;i<=10;i++){
           // String lockNode = zooKeeper.create(LOCK_NODE+ "/"+ "lock-", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            String sessionId = UUID.randomUUID().toString();
            String lockNode =  zooKeeper.create(LOCK_NODE+ "/"+ "lock-", sessionId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            System.out.println("lockNode "+lockNode);
        }
*/


        while(true) {
            String sessionId = UUID.randomUUID().toString();
            String lockNode = zooKeeper.create(LOCK_NODE+ "/"+ "lock-", sessionId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

          //  String lockNode = zooKeeper.create(LOCK_NODE+ "/"+ "lock-", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
           // String lockNode =  zooKeeper.create(LOCK_NODE+ "/"+ "lock-", sessionId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            //String lockNode =  zooKeeper.create(LOCK_NODE + "/lock", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            System.out.println("LockNOde : "+lockNode);
            List<String> children = zooKeeper.getChildren(ZNODE_NAMESPACE, false);

             if(children.size() > 0) {

                 // Sort the list of item nodes in ascending order
                 children.sort(String::compareTo);
                 String nodePath = ZNODE_NAMESPACE + "/" + children.get(0);


                 // Lock **********


                     List<String> childrenForLockNode = zooKeeper.getChildren(LOCK_NODE, false);
                 childrenForLockNode.sort(String::compareTo);

                 //int index = childrenForLockNode.indexOf(LOCK_NODE.substring(LOCK_NODE.lastIndexOf('/') + 1));

                 byte[] data = zooKeeper.getData(LOCK_NODE + "/" + childrenForLockNode.get(0), false, null);

                 System.out.println("Data :"+new String(data) +" sessionId :"+sessionId);
                 if (data != null && new String(data).equalsIgnoreCase(sessionId)) {
                 //if(index == 0) {
                     System.out.println("I acquired a lock ");

                     byte[] producerData = zooKeeper.getData(nodePath, false, null);
                     String item = new String(producerData);

                     LOG.info("Item : " + item);
                     //Thread.sleep(1000);
                     zooKeeper.delete(nodePath, -1);
                     zooKeeper.delete(LOCK_NODE + "/" + childrenForLockNode.get(0), -1);
                     //zooKeeper.delete(LOCK_NODE, -1);


                     // unlock
                 } else {
                     //zooKeeper.delete(LOCK_NODE, -1);
                     //zooKeeper.delete(LOCK_NODE + "/" + childrenForLockNode.get(0), -1);


                     // System.out.println("Could not get lock : " + sessionId);
                     zooKeeper.getChildren(LOCK_NODE, true);
                 }
             }
             else{
                 System.out.println("Waiting for input data from producer .........");
             }
        }
    }
}


