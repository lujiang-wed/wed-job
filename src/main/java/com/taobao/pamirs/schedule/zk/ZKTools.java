package com.taobao.pamirs.schedule.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * modify to curator
 * @author wednesday by 2019-07-15
 */
public class ZKTools {
    public static void createPath(CuratorFramework zk, String path, CreateMode createMode, List<ACL> acl) throws Exception {
        String[] list = path.split("/");
        String zkPath = "";
        for (String str : list) {
            if (!"".equals(str)) {
                zkPath = zkPath + "/" + str;
                if (zk.checkExists().forPath(zkPath) == null) {
                    zk.create()
                            .withMode(createMode)
                            .withACL(acl).forPath(zkPath, null);
                }
            }
        }
    }

    public static void printTree(CuratorFramework zk, String path, Writer writer, String lineSplitChar) throws Exception {
        String[] list = getTree(zk, path);
        Stat stat = new Stat();
        for (String name : list) {
            byte[] value = zk.getData().storingStatIn(stat).forPath(name);
            if (value == null) {
                writer.write(name + lineSplitChar);
            } else {
                writer.write(name + "[v." + stat.getVersion() + "]" + "[" + new String(value) + "]" + lineSplitChar);
            }
        }
    }

    public static void deleteTree(CuratorFramework zk, String path) throws Exception {
        zk.delete().deletingChildrenIfNeeded().forPath(path);
    }

    public static String[] getTree(CuratorFramework zk, String path) throws Exception {
        if (zk.checkExists().forPath(path) == null) {
            return new String[0];
        }
        List<String> dealList = new ArrayList<String>();
        dealList.add(path);
        int index = 0;
        while (index < dealList.size()) {
            String tempPath = dealList.get(index);
            List<String> children = zk.getChildren().forPath(tempPath);
            if (!"/".equalsIgnoreCase(tempPath)) {
                tempPath = tempPath + "/";
            }
            Collections.sort(children);
            for (int i = children.size() - 1; i >= 0; i--) {
                dealList.add(index + 1, tempPath + children.get(i));
            }
            index++;
        }
        return dealList.toArray(new String[0]);
    }
}
