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
    private static final String SYS_ROOT_PATH = "/wed-job";

    private Properties       properties;
    private CuratorFramework zk;

    public enum keys {
        //链接串IP:PORT,逗号分隔
        zkConnectString,
        //任务根目录，不包含/wed-job
        rootPath,
        //数据auth用户名，默认admin
        userName,
        //数据auth密码，默认admin
        password,
        //zk链接超时，默认20s
        zkSessionTimeout
    }

    private List<ACL> acl = new ArrayList<>();

    public ZKManager(Properties aProperties) throws Exception {
        properties = aProperties;
        this.connect();
    }

    private void connect() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                2000,
                5,
                Integer.valueOf(properties.getProperty(keys.zkSessionTimeout.toString()))
        );
        // 默认用户名：root 密码：root
        String authString = properties.getProperty(keys.userName.toString())
                + ":" + properties.getProperty(keys.password.toString());


        zk = CuratorFrameworkFactory.builder()
                .connectString(properties.getProperty(keys.zkConnectString.toString()))
                .retryPolicy(retryPolicy)
                .authorization("digest", authString.getBytes())
                .build();

        zk.start();

        createZookeeper(zk);

        acl.clear();
        acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest",
                DigestAuthenticationProvider.generateDigest(authString))));
        acl.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));

        zk.setACL().withACL(acl);
    }

    private void createZookeeper(CuratorFramework zkClient) throws Exception {
        if (zkClient != null) {
            Stat sysPath = zkClient.checkExists().forPath(SYS_ROOT_PATH);
            Stat rootPath = zkClient.checkExists().forPath(SYS_ROOT_PATH + properties.getProperty(keys.rootPath.toString()));
            if (sysPath == null) {
                zkClient.create()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(SYS_ROOT_PATH, Version.getVersion().getBytes());
            }
            if (rootPath == null) {
                zkClient.create()
                        .creatingParentContainersIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(SYS_ROOT_PATH + properties.getProperty(keys.rootPath.toString()), null);
            }

        } else {
            throw new RuntimeException("[WED-JOB]zookeeper链接失败...");
        }
    }

    public static Properties createProperties() {
        Properties result = new Properties();
        result.setProperty(keys.zkConnectString.toString(), "19.19.22.51:2181,19.19.22.52:2181,19.19.22.53:2181");
        result.setProperty(keys.rootPath.toString(), SYS_ROOT_PATH + "/tasks_center");
        result.setProperty(keys.userName.toString(), "admin");
        result.setProperty(keys.password.toString(), "admin");
        result.setProperty(keys.zkSessionTimeout.toString(), "60000");
        return result;
    }

    public String getDefaultPath() {
        return SYS_ROOT_PATH + properties.getProperty(keys.rootPath.toString());
    }

    public String getConnectStr() {
        return properties.getProperty(keys.zkConnectString.toString());
    }

    public boolean checkZookeeperState() {
        return zk != null && zk.getState().equals(CuratorFrameworkState.STARTED);
    }

    public void close() {
        logger.info("[WED-JOB]zookeeper shutdown...");
        this.zk.close();
    }

    public CuratorFramework getZkClient() {
        return zk;
    }

    public List<ACL> getAcl() {
        return acl;
    }
}
