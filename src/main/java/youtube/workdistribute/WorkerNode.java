package youtube.workdistribute;


import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;


public class WorkerNode {
    static ZooKeeper zooKeeper;
    private static final Logger LOG = LoggerFactory.getLogger(WorkerNode.class);
    private static final int NUMBER_OF_TASK = 1000;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        String machine_id = UUID.randomUUID().toString();
        String rootNode = "/dbprocessing";
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                LOG.info("*********************************************");
                if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                    try {
                        distributeWork(machine_id, rootNode);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                
            }
        };

        zooKeeper = new ZooKeeper("localhost:2181", 20000, watcher);

        if(zooKeeper.exists(rootNode, false) ==  null){
            zooKeeper.create(rootNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        LOG.info("my id  = "+ machine_id);

        zooKeeper.create(rootNode+ "/"+ "worker-"+machine_id, machine_id.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        //getLockOrWait(sessionId, rootNode);
        distributeWork(machine_id, rootNode);
        Thread.sleep(100_000_000);
    }

    private static void distributeWork(String machine_id, String rootNode) throws KeeperException, InterruptedException {
        LOG.info("I am : "+machine_id);
        List<String> children = zooKeeper.getChildren(rootNode, false);
        children.sort(String::compareTo);
        byte[] data = zooKeeper.getData(rootNode+ "/"+children.get(0), false, null);
        LOG.info("Data : "+data);
        int n = children.size();
        LOG.info("Number of worker connected :"+n);
        int portion = NUMBER_OF_TASK/n;
        int startFrom = 0;
        for(String child : children){
            LOG.info("Child : "+child);
            LOG.info(child +" your range : "+startFrom +" To "+ (startFrom+portion));
            startFrom+=portion+1;
        }

        if(data!=null && new String(data).equalsIgnoreCase(machine_id)){

        }
       zooKeeper.getChildren(rootNode, true);
    }
}