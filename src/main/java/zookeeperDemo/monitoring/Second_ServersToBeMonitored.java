package zookeeperDemo.monitoring;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Second_ServersToBeMonitored {
    private static String MembersNode = "/members";
    private static final Logger LOG = LoggerFactory.getLogger(Second_ServersToBeMonitored.class);

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String id =  UUID.randomUUID().toString();
        LOG.info("my id  = "+ id);
        ZooKeeper zookeeper = new ZooKeeper("localhost:2181", 15000, null);
        String creationResponse = zookeeper.create(MembersNode+"/"+ id, id.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, null);
        LOG.info(creationResponse);
        Thread.sleep(100_000_000);
    }
}