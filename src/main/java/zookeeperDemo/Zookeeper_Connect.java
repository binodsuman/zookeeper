package zookeeperDemo;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
public class Zookeeper_Connect {
    private static final Logger LOG = LoggerFactory.getLogger(Zookeeper_Connect.class);
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final String ROOT_ZNODE = "/demo_test";
    static ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

      zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, 20000, null);

        if(zooKeeper.exists(ROOT_ZNODE, false) ==  null){
            zooKeeper.create(ROOT_ZNODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOG.info(ROOT_ZNODE + " root node has been created");
        }

        if(zooKeeper.exists(ROOT_ZNODE+ "/"+ "child_node_test" , false) == null) {
            zooKeeper.create(ROOT_ZNODE + "/" + "child_node_test", "Demo_data_test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOG.info(ROOT_ZNODE + "/" + "child_node" + " Child node has been created");
        }

        List<String> children = zooKeeper.getChildren(ROOT_ZNODE, false);
        LOG.info("Number of Child node : "+children.size());
        for(String child : children){
            LOG.info("Child : "+child);
        }

        //Thread.sleep(100_000_000);
    }


}
