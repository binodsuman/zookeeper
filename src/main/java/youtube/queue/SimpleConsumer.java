package youtube.queue;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {
    static ZooKeeper zooKeeper;
    private static final String ZNODE_NAMESPACE = "/queue_1";

    private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumer.class);
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String sessionId = UUID.randomUUID().toString();

        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //System.out.println("Event type : "+watchedEvent.getType());
                if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                    try {
                        //System.out.println("some change in children node");
                        dataConsume();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    }
                }
          }
        };

        zooKeeper = new ZooKeeper("localhost:2181", 20000, watcher);
       if(zooKeeper.exists(ZNODE_NAMESPACE, false) ==  null){
            zooKeeper.create(ZNODE_NAMESPACE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
       dataConsume();

       Thread.sleep(100_100_100);
    }

    private static void dataConsume() throws InterruptedException, KeeperException {

        //System.out.println("Data pushed by Producer ******   5  ***");
        List<String> children = zooKeeper.getChildren(ZNODE_NAMESPACE, true);
        if(children.size() > 0) {
                byte[] producerData = zooKeeper.getData(ZNODE_NAMESPACE + "/" + children.get(0), false, null);
                String item = new String(producerData);
                LOG.info("Item : " + item);
                zooKeeper.delete(ZNODE_NAMESPACE + "/" + children.get(0), -1);
               // Not required as by deleting znode in above, will trigger call back.
               // zooKeeper.getChildren(ZNODE_NAMESPACE, true);

        }else{
            //System.out.println("Waiting for input data from producer .........");
            zooKeeper.getChildren(ZNODE_NAMESPACE, true);
        }
    }
}