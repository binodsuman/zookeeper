package zookeeperDemo.leader;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;


public class Leader_Selection {
    static ZooKeeper zooKeeper;
    private static final Logger LOG = LoggerFactory.getLogger(Leader_Selection.class);
    private static final int NUMBER_OF_TASK = 1000;
    private static final String ELECTION_NAMESPACE = "/election";
    private String currentZnodeName = "";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        Leader_Selection leaderElection = new Leader_Selection();
        leaderElection.connectToZookeeper();
        leaderElection.nominateForLeadership();

        Thread.sleep(100_000_000);
    }

    private void connectToZookeeper() throws IOException {
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                LOG.info("************************ : "+watchedEvent.getType());
                if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                    try {
                        leaderSelectionLogic(ELECTION_NAMESPACE,currentZnodeName);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };

        zooKeeper = new ZooKeeper("localhost:2181", 20000, watcher);

    }

    private void nominateForLeadership() throws InterruptedException, KeeperException {
        if(zooKeeper.exists(ELECTION_NAMESPACE, false) ==  null){
            zooKeeper.create(ELECTION_NAMESPACE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }


        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        LOG.info("znode name " + znodeFullPath);
        currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
        LOG.info("I am : " + currentZnodeName);
        leaderSelectionLogic(ELECTION_NAMESPACE, currentZnodeName);

    }

    private static void leaderSelectionLogic(String ELECTION_NAMESPACE, String currentZnodeName) throws KeeperException, InterruptedException {

        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(children);
        String smallestChild = children.get(0);
        System.out.println("smallestChild : "+smallestChild);
        System.out.println("currentZnodeName : "+currentZnodeName);
        if (smallestChild.equals(currentZnodeName)) {
            LOG.info("I am the leader, I can execute some task");
           /* for(int i=0;i<10;i++){
                System.out.println(i);
                Thread.sleep(1000);
            }*/

        } else {
            //String currentLeader = getCurrentLeader(currentZnodeName);
            LOG.info("I am not the leader, Leader is "+smallestChild);
            zooKeeper.getChildren(ELECTION_NAMESPACE, true);

        }


    }

    private static String getCurrentLeader(String currentZnodeName) throws InterruptedException, KeeperException {

        Stat predecessorStat = null;
        String predecessorZnodeName = "";
        while (predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

            int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
            predecessorZnodeName = children.get(predecessorIndex);
            predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, null);
        }
        return predecessorZnodeName;
    }
}