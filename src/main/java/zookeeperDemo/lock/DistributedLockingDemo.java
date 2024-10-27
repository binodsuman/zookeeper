package zookeeperDemo.lock;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class DistributedLockingDemo {
    static ZooKeeper zooKeeper;
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String nodeID = UUID.randomUUID().toString();
        String lockParentNode = "/DLock";
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("Event type : "+watchedEvent.getType());
                //if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                    try {
                        getLockOrWait(nodeID, lockParentNode);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
               // }


            }
        };

        System.out.println("My Id : "+nodeID);
        zooKeeper = new ZooKeeper("localhost:2181", 1000, watcher);

        if(zooKeeper.exists(lockParentNode, false) ==  null){
            zooKeeper.create(lockParentNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        zooKeeper.create(lockParentNode+ "/"+ "lock-", nodeID.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        //getLockOrWait(nodeID, lockParentNode);
        Thread.sleep(100_100_100);
    }

    private static void getLockOrWait(String nodeID, String rootNode) throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(rootNode, true);
        System.out.println("Number of Nodes in waiting : "+children.size());
        if(children.size() > 0) {
            children.sort(String::compareTo);
            byte[] data = zooKeeper.getData(rootNode + "/" + children.get(0), false, null);
            if (data != null && new String(data).equalsIgnoreCase(nodeID)) {
                System.out.println(children.get(0) +" I acquired a lock, will leave it in 10 seconds");
                lockFunction();
                zooKeeper.delete(rootNode + "/" + children.get(0), -1);
            } else {
                System.out.println("Lock acquired by " + nodeID + " I could not acquire a lock. So will wait");
                //zooKeeper.getChildren(rootNode, true);
            }
        }


    }

    private static void lockFunction() throws InterruptedException {
        for (int i=0;i<10;i++){
            System.out.println("leaving in "+ i + " seconds");
            Thread.sleep(1500);
        }
    }
}