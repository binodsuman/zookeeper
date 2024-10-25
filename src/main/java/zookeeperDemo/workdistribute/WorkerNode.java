package zookeeperDemo.workdistribute;


import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;


public class WorkerNode {
    static ZooKeeper zooKeeper;
    private static final Logger LOG = LoggerFactory.getLogger(WorkerNode.class);
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final String ROOT_ZNODE = "/dbprocessing";

    private static final int NUMBER_OF_TASK = 1000;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        // In Actual System, this should be IP address of Worker node.
        String machine_id = UUID.randomUUID().toString();

        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                LOG.info("*********************************************");
                if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                    try {
                        distributeWork(machine_id, ROOT_ZNODE);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                
            }
        };

        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, 20000, watcher);

        if(zooKeeper.exists(ROOT_ZNODE, false) ==  null){
            zooKeeper.create(ROOT_ZNODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        LOG.info("This Machine id  = "+ machine_id);

        zooKeeper.create(ROOT_ZNODE+ "/"+ "worker-"+machine_id, machine_id.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        distributeWork(machine_id, ROOT_ZNODE);
        Thread.sleep(100_000_000);
    }

    private static void distributeWork(String machine_id, String rootNode) throws KeeperException, InterruptedException {
        //LOG.info("I am : worker-"+machine_id);
        List<String> children = zooKeeper.getChildren(rootNode, false);
        children.sort(String::compareTo);
        byte[] data = zooKeeper.getData(rootNode+ "/"+children.get(0), false, null);
        //LOG.info("Data : "+data);
        int n = children.size();
        LOG.info("Number of worker connected :"+n);
        int portion = NUMBER_OF_TASK/n;
        int startFrom = 0;
        for(String child : children){
            //LOG.info("Child : "+child);
            if(child.startsWith("worker-"+machine_id)){
                LOG.info(" your range : "+startFrom +" To "+ (startFrom+portion));
            }
            startFrom+=portion+1;
        }

        if(data!=null && new String(data).equalsIgnoreCase(machine_id)){

        }
       zooKeeper.getChildren(rootNode, true);
    }
}