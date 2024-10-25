package zookeeperDemo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class Ephemeral_Node_Creation {
    //private static final Logger LOG = LoggerFactory.getLogger(Ephemeral_Node_Creation.class);
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181,localhost:2182,localhost:2183";
    private static final String ROOT_ZNODE = "/demo";
    static ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

      String nodeName = args[0];
      System.out.println("Entered Node Name : "+nodeName);

      zooKeeper = new ZooKeeper("localhost:2181", 20000, null);

        if(zooKeeper.exists("/"+nodeName, false) ==  null){
            zooKeeper.create("/"+nodeName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println(nodeName + "  node has been created");
        }



        Thread.sleep(100_000_000);
    }


}
