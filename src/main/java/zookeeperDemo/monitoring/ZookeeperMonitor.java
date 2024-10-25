package zookeeperDemo.monitoring;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class ZookeeperMonitor {
    private static String MembersNode = "/members";
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperMonitor.class);
    private static ZooKeeper zookeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        zookeeper = new ZooKeeper("localhost:2181", 15000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                LOG.info("got the event for node = "+ watchedEvent.getPath());
                LOG.info("the event type = "+ watchedEvent.getType());
                 LOG.info("*********************************************************");
                try {
                    startWatch();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // CREATE
        if(zookeeper.exists(MembersNode, false) == null){
            zookeeper.create(MembersNode, "data".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // WATCH FOR CHILD NODES
        //startWatch();

        Thread.sleep(100_000_000);
    }

    private static void startWatch() throws InterruptedException, KeeperException {
        if(zookeeper!=null){
            DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
            List<String> childrens  =zookeeper.getChildren(MembersNode, true);
            /*Stat stat = zookeeper.exists(MembersNode,null);
            long a = stat.getCtime();
            System.out.println("Stat : "+stat);
            System.out.println("Created from :"+a);
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(a);
            System.out.println(formatter.format(calendar.getTime()));


            Date date = new Date(a);
            System.out.println("Date : "+date); */

            System.out.println("List of children = "+childrens.size());
            for(String child : childrens){
                System.out.println(MembersNode+ "/"+child);
                Stat stat = zookeeper.exists(MembersNode+ "/"+child,null);
                long milliSecondsEpochTime = stat.getCtime();
                Date date = new Date(milliSecondsEpochTime);
                System.out.println("Alive Since : "+date);
            }

            //System.out.println("List of children = "+childrens.size());
            //childrens.forEach(c -> System.out.print(c+" \n"));
            System.out.println();
        }
    }
}