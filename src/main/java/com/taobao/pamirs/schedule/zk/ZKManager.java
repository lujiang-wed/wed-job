package com.taobao.pamirs.schedule.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ZKManager {

    private static final Logger logger        = LoggerFactory.getLogger(ZKManager.class);
    private static final String SYS_ROOT_PATH = "wed-job";
    private Properties       zkProperties;
    private CuratorFramework zkClient;
    private List<ACL> acl = new ArrayList<>();

    public ZKManager(Properties properties) throws Exception {
        this.zkProperties = properties;
        this.connect();
    }

    /**
     * 连接zk集群
     *
     * @throws Exception zk异常抛出便于定位问题
     */
    private void connect() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                2000,
                5,
                Integer.valueOf(this.zkProperties.getProperty(PropertyKeys.getByName(PropertyKeys.ZK_SESSIONT_IMEOUT)))
        );
        // 默认用户名：root 密码：root
        String authString = this.zkProperties.getProperty(PropertyKeys.getByName(PropertyKeys.USER_NAME))
                + ":" + this.zkProperties.getProperty(PropertyKeys.getByName(PropertyKeys.PASSWORD));


        zkClient = CuratorFrameworkFactory.builder()
                .connectString(this.zkProperties.getProperty(PropertyKeys.getByName(PropertyKeys.ZK_CONNECT_STRING)))
                .retryPolicy(retryPolicy)
                .authorization("digest", authString.getBytes())
                .build();

        acl.clear();
        acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest",
                DigestAuthenticationProvider.generateDigest(authString))));
        acl.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));

        zkClient.setACL().withACL(acl);
        zkClient.start();

        createZookeeper(zkClient);

    }

    public CuratorFramework getZkClient() {
        return zkClient;
    }

    private void createZookeeper(CuratorFramework zkClient) throws Exception {
        if (zkClient != null) {
            Stat sysPath = zkClient.checkExists().forPath(SYS_ROOT_PATH);
            if (sysPath == null) {
                zkClient.create()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(SYS_ROOT_PATH, Version.getVersion().getBytes());
            }
            zkClient.create()
                    .creatingParentContainersIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(this.zkProperties.getProperty(PropertyKeys.getByName(PropertyKeys.ROOT_PATH)
                    ));

        } else {
            throw new RuntimeException("[WED-JOB]zookeeper链接失败...");
        }
    }


    public void close() {
        logger.info("[WED-JOB]zookeeper shutdown...");
        this.zkClient.close();
    }

    public static Properties createProperties() {
        Properties result = new Properties();
        result.setProperty(PropertyKeys.getByName(PropertyKeys.ZK_CONNECT_STRING), "localhost:2181");
        result.setProperty(PropertyKeys.getByName(PropertyKeys.ROOT_PATH), SYS_ROOT_PATH + "/default");
        result.setProperty(PropertyKeys.getByName(PropertyKeys.USER_NAME), "root");
        result.setProperty(PropertyKeys.getByName(PropertyKeys.PASSWORD), "root");
        result.setProperty(PropertyKeys.getByName(PropertyKeys.ZK_SESSIONT_IMEOUT), "60000");
        return result;
    }

    public String getDefaultPath() {
        return this.zkProperties.getProperty(PropertyKeys.getByName(PropertyKeys.ROOT_PATH));
    }

    public String getConnectStr() {
        return this.zkProperties.getProperty(PropertyKeys.getByName(PropertyKeys.ZK_CONNECT_STRING));
    }

    public boolean checkZookeeperState() throws Exception {
        return zkClient != null && zkClient.getState().compareTo(CuratorFrameworkState.STARTED) > 0;
    }
}
