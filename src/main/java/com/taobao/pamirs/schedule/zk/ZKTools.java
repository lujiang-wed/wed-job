package com.taobao.pamirs.schedule.zk;

import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ZKTools {
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
