package zookeeperDemo;

import org.apache.zookeeper.*;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import java.io.IOException;

public class TTL_Node_Creation {
    private static final Logger LOG = LoggerFactory.getLogger(TTL_Node_Creation.class);
    //private static final String ZOOKEEPER_ADDRESS = "localhost:2181,localhost:2182,localhost:2183";
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    static ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {


        String nodeName = args[0];
        System.out.println("Entered Node Name : "+nodeName);

        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                //LOG.info("*********************************************");
                LOG.info("*********"+ watchedEvent.getState());
                System.out.println(watchedEvent.getType());
                //Event.EventType.NodeDeleted
                if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){

                    System.out.println("** Callback 1 **");
                    System.out.println("/"+nodeName+"/foo2" +" : DELETED");

                }


            }
        };

        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, 20000, watcher);

        if(zooKeeper.exists("/"+nodeName, false) ==  null){
            zooKeeper.create("/"+nodeName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(nodeName + "  node has been created");
        }


        zooKeeper.create("/"+nodeName+"/foo2", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_WITH_TTL,  null, 2000);
        System.out.println(nodeName + " TTL node has been created");

        ttlNodeDeleted("/"+nodeName);
        Thread.sleep(100_000_000);
    }

    private static void ttlNodeDeleted( String rootNode) throws KeeperException, InterruptedException {
        zooKeeper.getChildren(rootNode, true);
    }

}

