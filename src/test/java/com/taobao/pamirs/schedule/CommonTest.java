package com.taobao.pamirs.schedule;

import com.taobao.pamirs.schedule.strategy.TBScheduleManagerFactory;
import com.taobao.pamirs.schedule.zk.ZKManager;
import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.*;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class CommonTest {
//    @Test
//    public static void main(String... args) throws IOException, KeeperException, InterruptedException {
//        ZooKeeper zk = new ZooKeeper("172.0.0.1:2181", 30000, new Watcher() {
//            @Override
//            public void process(WatchedEvent watchedEvent) {
//                if (watchedEvent.getState().equals(Event.KeeperState.SyncConnected)) {
//                    System.out.println("zookeeper connected......");
//                }
//            }
//        });
//        IZkConnection zkConnection = new ZkConnection("19.19.9.170:2181", 20000);
//        ZkClient zk = new ZkClient(zkConnection);
//
//        zk.waitUntilConnected(60, TimeUnit.SECONDS);
//
//
//        zk.create("/top001", "top001".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        zk.create("/top001/mid001", "mid001".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        zk.create("/top001/mid001/bottom001", "bottom001".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        if (!zk.exists("/")) {
//            return;
//        } else {
//            List<String> dealList = new ArrayList<String>();
//            dealList.add("/");
//            for (int i = 0; i <= zk.countChildren("/"); i++) {
//                String tempPath = dealList.get(i);
//                List<String> children = zk.getChildren(tempPath);
//                if (!tempPath.equalsIgnoreCase("/")) {
//                    tempPath = tempPath + "/";
//                }
//                Collections.sort(children);
//                for (String child : children) {
//                    dealList.add(tempPath + child);
//                }
//            }
//
//
//            int index = 0;
//            while (index < dealList.size()) {
//                String tempPath = dealList.get(index);
//                List<String> children = zk.getChildren(tempPath);
//                if (!tempPath.equalsIgnoreCase("/")) {
//                    tempPath = tempPath + "/";
//                }
//                Collections.sort(children);
//                for (int i = children.size() - 1; i >= 0; i--) {
//                    dealList.add(index + 1, tempPath + children.get(i));
//                }
//                index++;
//            }
//            System.out.println(Arrays.toString(dealList.toArray(new String[0])));
//        }
//    }
}