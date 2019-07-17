package com.taobao.pamirs.schedule.zk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleWatcher implements Watcher {
    private static transient Logger logger = LoggerFactory.getLogger(ScheduleWatcher.class);
    private Map<String, Watcher>    route  = new ConcurrentHashMap<String, Watcher>();
    private CuratorFramework manager;

    public ScheduleWatcher(CuratorFramework aManager) {
        this.manager = aManager;
    }

    public void registerChildrenChanged(String path, Watcher watcher) throws Exception {
        manager.getChildren().forPath(path);
        route.put(path, watcher);
    }

    public void process(WatchedEvent event) {
        if (logger.isInfoEnabled()) {
            logger.info("已经触发了" + event.getType() + ":" + event.getState() + "事件！" + event.getPath());
        }
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            String path = event.getPath();
            Watcher watcher = route.get(path);
            if (watcher != null) {
                try {
                    watcher.process(event);
                } finally {
                    try {
                        if (manager.checkExists().forPath(path) != null) {
                            manager.getChildren().forPath(path);
                        }
                    } catch (Exception e) {
                        logger.error(path + ":" + e.getMessage(), e);
                    }
                }
            } else {
                logger.info("已经触发了" + event.getType() + ":" + event.getState() + "事件！" + event.getPath());
            }
        } else if (event.getState() == KeeperState.SyncConnected) {
            logger.info("收到ZK连接成功事件！");
        } else if (event.getState() == KeeperState.Disconnected) {
            logger.info("链接断开，等待中心建立ZK链接");
            try {
                manager.start();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        } else if (event.getState() == KeeperState.Expired) {
            logger.error("会话超时，等待重新建立ZK连接...");
            try {
                manager.start();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
