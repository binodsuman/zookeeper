# zookeeper
All basic and system design code using Zookeeper, included:
Added basic about Zookeeper
Command for Zookeeper server start and stop
Command for Zookeeper client
How to create znode
How to build code
How to run the code

+ First install zookeeper
+ Think few problem statement:

1. How you coordinate the multiple server in distributed environment to 
    1. Solve 100K same task by multiple server without conflicting to each other
    2. How you monitor if some server die or not responding
    3. How to inform to other server if new server attached to network of scaling
2. Configuration management - How to multiple server will use same configuration file and get updated if any change in configuration file, without polling always from DB and GitHub.
3. If you make one mater and other slave in your network then how you will handle if master goes down. How effetely you will handle single point of failure.
4. How to maintain queue system in your server cluster, means which server come fast and some in second for some use case.
5. How to manage distributed counter service in multiple server environment.

Famous use case in distributed environment that can easily done by Zookeeper :
1. Leader Selection
2. Queue
3. Lock
4. Group membership
5. Service discovery


Zookeeper:
- Apache open source project in java.
- Was originally developed at Yahoo.
- Backend service.
- Provide a way for the client to implement tasks for distributed system.
- Used by many famous service like: Kafka, Hbase, Hadoop, Flink, Neo4j.


Installation:
Download Zookeeper 3.9.2
/Users/binod/Documents/software/apache-zookeeper-3.9.2-bin
Make one data folder in this directory, where zookeeper will store all their data.

Change in config/zoo.cfg
dataDir=/Users/binod/Documents/software/apache-zookeeper-3.9.2-bin/data

Few commands:
Server:
zkServer.sh start
zkServer.sh status
zkServer.sh stop

Client:
zkCli.sh - server localhost:2181
Ls /
Create /hello ‘Binod’
Delete /hello



Binods-MacBook-Pro:apache-zookeeper-3.9.2-bin binod$ bin/zkServer.sh start conf/zoo.cfg 
/usr/bin/java
ZooKeeper JMX enabled by default
Using config: conf/zoo.cfg
Starting zookeeper ... STARTED
Binods-MacBook-Pro:apache-zookeeper-3.9.2-bin binod$ 


Binods-MacBook-Pro:apache-zookeeper-3.9.2-bin binod$ bin/zkServer.sh status conf/zoo.cfg 
/usr/bin/java
ZooKeeper JMX enabled by default
Using config: conf/zoo.cfg
Client port found: 2181. Client address: localhost. Client SSL: false.
Mode: standalone
Binods-MacBook-Pro:apache-zookeeper-3.9.2-bin binod$ 


On another terminal:
Binods-MacBook-Pro:apache-zookeeper-3.9.2-bin binod$ bin/zkCli.sh -server localhost:2181
/usr/bin/java
Connecting to localhost:2181
2024-09-25 07:54:15,393 [myid:] - INFO  [main:o.a.z.Environment@98] - Client environment:zookeeper.version=3.9.2-e454e8c7283100c7caec6dcae2bc82aaecb63023, built on 2024-02-12 20:59 UTC


Create some data on Server from client terminal:
[zk: localhost:2181(CONNECTED) 0] create /mydata "Binod Suman"
Created /mydata
[zk: localhost:2181(CONNECTED) 1] ls /
[mydata, zookeeper]
[zk: localhost:2181(CONNECTED) 3] get /mydata
Binod Suman
[zk: localhost:2181(CONNECTED) 4] 


Delete Znode with having so many clild
zkCli.sh -server xxx deleteall /test
bin/zkCli.sh -server localhost:2181  deleteall /queue



** How to run the code **

source ~/.bash_profile
mvn clean install
mvn exec:java -Dexec.mainClass=youtube.queue.Producer
mvn exec:java -Dexec.mainClass=youtube.queue.Consumer

You can run multiple node:
mvn exec:java -Dexec.mainClass=youtube.leader.LeaderElection
mvn exec:java -Dexec.mainClass=youtube.leader.LeaderElection
mvn exec:java -Dexec.mainClass=youtube.leader.LeaderElection
mvn exec:java -Dexec.mainClass=youtube.leader.LeaderElection





