package zookeeperDemo.workdistribute;


import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;


public class WorkerNode {
    static ZooKeeper zooKeeper;
    private static final Logger LOG = LoggerFactory.getLogger(WorkerNode.class);
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final String ROOT_ZNODE = "/db";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
       // In Actual System, this should be IP address of Worker node.
        String machine_id = UUID.randomUUID().toString();

        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                 if((watchedEvent.getType() == Event.EventType.NodeChildrenChanged) ||
                   (watchedEvent.getType() == Event.EventType.NodeDataChanged)) {
                    try {
                        distributeWork(machine_id, ROOT_ZNODE);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException | UnsupportedEncodingException e) {
                        throw new RuntimeException(e);
                    }
                }
          }
        };

        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, 1000, watcher);

        if(zooKeeper.exists(ROOT_ZNODE,null) ==  null){
            zooKeeper.create(ROOT_ZNODE, "1000".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        LOG.info("This Machine id  = "+ machine_id);

        zooKeeper.create(ROOT_ZNODE+ "/"+ "worker-"+machine_id, machine_id.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        distributeWork(machine_id, ROOT_ZNODE);
        Thread.sleep(100_000_000);
    }

    private static void distributeWork(String machine_id, String rootNode) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        List<String> children = zooKeeper.getChildren(rootNode, true);
        byte[] dataByte = zooKeeper.getData(rootNode,true,null);
        String data = new String(dataByte,"UTF-8");

        int number_of_task = Integer.parseInt(data.toString());
        System.out.println("Number of task in configuration : "+number_of_task);
        int n = children.size();
        LOG.info("Number of worker connected :"+n);
        int portion = number_of_task/n;
        int startFrom = 0;
        for(String child : children){
            if(child.startsWith("worker-"+machine_id)){
                LOG.info(" your range now now : "+startFrom +" To "+ (startFrom+portion));
            }
            startFrom = startFrom + portion+1;
        }

    }
}