package zookeeperDemo.lock;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LocksDemo implements Watcher{
    static ZooKeeper zooKeeper;
    String lockZNode = "/locks";
    String currentZnodeName = "";


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LocksDemo demo = new LocksDemo();
        demo.serverConnection();
    }

    public void serverConnection() throws IOException, InterruptedException, KeeperException {
        zooKeeper =new ZooKeeper("localhost:2181",20000,this);
        if(zooKeeper.exists(lockZNode,false)==null) {
            zooKeeper.create(lockZNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        String znodeFullPath = zooKeeper.create(lockZNode+"/"+"lock-",new byte[]{},ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        this.currentZnodeName = znodeFullPath.replace(lockZNode + "/", "");
        System.out.println("currentZnodeName " + this.currentZnodeName);
        getLockOrWait(currentZnodeName, lockZNode);
    }

    public void process(WatchedEvent event) {
        System.out.println(event.getType());
            switch (event.getType()) {
                case None:
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        System.out.println("Successfully connected to Zookeeper ");
                    }
                    break;
                case NodeChildrenChanged:
                    try {
                        System.out.println("Children node changed *******");
                        getLockOrWait(currentZnodeName, lockZNode);
                    } catch (InterruptedException e) {
                    } catch (KeeperException e) {
                    }
                case NodeDeleted:
                    try {
                        System.out.println("One children znode deleted *******");
                        getLockOrWait(currentZnodeName, lockZNode);
                    } catch (InterruptedException e) {
                    } catch (KeeperException e) {
                    }

            }
        }

    private  void getLockOrWait(String currentZnodeName, String rootNode) throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(rootNode, false);
        Collections.sort(children);
        String mostSeniorNodeAtPresent = children.get(0);
        System.out.println("mostSeniorNodeAtPresent : "+mostSeniorNodeAtPresent);

        if (mostSeniorNodeAtPresent.equals(currentZnodeName)) {

            System.out.println("I acquired a lock  by Server till this node alive");
            int i = 0;
            while(i < Integer.MAX_VALUE){
                System.out.println("Processing by leader node "+ i++ + "seconds");
                Thread.sleep(1000);
            }
           // zooKeeper.delete(rootNode+ "/"+children.get(0), -1);
            zooKeeper.exists(rootNode + "/" + mostSeniorNodeAtPresent, this);
        }else{
            System.out.println("i could not acquire a lock. So will wait");
            // getChildren will call NodeChildrenChanged watcher.
            //zooKeeper.getChildren(rootNode, true);
            // exist will call NodeDeleted watcher.
            zooKeeper.exists(rootNode + "/" + mostSeniorNodeAtPresent, this);
        }

        Thread.sleep(100_100_100);
    }
}