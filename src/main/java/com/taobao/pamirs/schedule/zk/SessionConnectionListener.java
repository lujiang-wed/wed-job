package com.taobao.pamirs.schedule.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Wednesday luj@jccfc.com
 * @version V1.0.0
 * @Title: SessionConnectionListener
 * @Package com.taobao.pamirs.schedule.zk
 * @Description:
 * @date 2019/1/14 17:28
 */
public class SessionConnectionListener implements ConnectionStateListener {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private String path;
    private String data;

    public SessionConnectionListener(String path, String data) {
        this.path = path;
        this.data = data;
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (newState == ConnectionState.LOST) {
            logger.error("[WED-JOB]zookeeper session expired...");
            while (true) {
                try {
                    if (client.getZookeeperClient().blockUntilConnectedOrTimedOut()) {
                        client.create()
                                .creatingParentsIfNeeded()
                                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                                .forPath(path, data.getBytes());
                        logger.info("[WED-JOB]zookeeper session rebuild...");
                        break;
                    }
                } catch (Exception e) {
                    logger.info("[WED-JOB]zookeeper session lost...");
                    throw new RuntimeException("[WED-JOB]zookeeper session lost...");
                }
            }
        }
    }
}