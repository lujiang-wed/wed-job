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
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ZKManager {

    private static transient Logger log = LoggerFactory.getLogger(ZKManager.class);
    private CuratorFramework zk;
    private List<ACL> acl = new ArrayList<ACL>();
    private Properties properties;

    public enum keys {
        //zookeeper链接串，无协议头格式。
        zkConnectString,
        //数据根目录，默认添加wed-job
        rootPath,
        //用户名
        userName,
        //密码
        password,
        //链接超时
        zkSessionTimeout
    }

    public ZKManager(Properties aProperties) throws Exception {
        this.properties = aProperties;
        this.connect();
    }

    /**
     *
     * 重连zookeeper
     *
     */
    public synchronized void reConnection() throws Exception {
        if (this.zk != null) {
            this.zk.close();
            this.zk = null;
            this.connect();
        }
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

        createZookeeper();

        acl.clear();
        acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest",
                DigestAuthenticationProvider.generateDigest(authString))));
        acl.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));

        zk.setACL().withACL(acl);
    }

    private void createZookeeper() throws Exception {
        initial();
    }


    public void close() {
        log.info("关闭zookeeper连接");
        if (zk == null) {
            return;
        }
        this.zk.close();
    }

    public void initial() throws Exception {
        //当zk状态正常后才能调用
        if (zk.checkExists().forPath(this.getRootPath()) == null) {
            zk.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(this.getAcl()).forPath(this.getRootPath());
            checkParent(zk, this.getRootPath());
            //设置版本信息
            zk.setData().forPath(this.getRootPath(), Version.getVersion().getBytes()).setVersion(-1);
        } else {
            //先校验父亲节点，本身是否已经是schedule的目录
            checkParent(zk, this.getRootPath());
            byte[] value = zk.getData().forPath(this.getRootPath());
            if (value == null) {
                zk.setData().forPath(this.getRootPath(), Version.getVersion().getBytes()).setVersion(-1);
            } else {
                String dataVersion = new String(value);
                if (!Version.isCompatible(dataVersion)) {
                    throw new Exception("TBSchedule程序版本 " + Version.getVersion() + " 不兼容Zookeeper中的数据版本 " + dataVersion);
                }
                log.info("当前的程序版本:" + Version.getVersion() + " 数据版本: " + dataVersion);
            }
        }
    }

    public static void checkParent(CuratorFramework zk, String path) throws Exception {
        String[] list = path.split("/");
        String zkPath = "";
        for (int i = 0; i < list.length - 1; i++) {
            String str = list[i];
            if (!"".equals(str)) {
                zkPath = zkPath + "/" + str;
                if (zk.checkExists().forPath(zkPath) != null) {
                    byte[] value = zk.getData().forPath(zkPath);
                    if (value != null) {
                        String tmpVersion = new String(value);
                        if (tmpVersion.contains("taobao-pamirs-schedule-")) {
                            throw new Exception("\"" + zkPath + "\"  is already a schedule instance's root directory, its any subdirectory cannot as the root directory of others");
                        }
                    }
                }
            }
        }
    }

    public List<ACL> getAcl() {
        return acl;
    }

    public CuratorFramework getZooKeeper() throws Exception {
        if (!this.checkZookeeperState()) {
            reConnection();
        }
        return this.zk;
    }

    public static Properties createProperties() {
        Properties result = new Properties();
        result.setProperty(keys.zkConnectString.toString(), "localhost:2181");
        result.setProperty(keys.rootPath.toString(), "/taobao-pamirs-schedule/huijin");
        result.setProperty(keys.userName.toString(), "ScheduleAdmin");
        result.setProperty(keys.password.toString(), "password");
        result.setProperty(keys.zkSessionTimeout.toString(), "60000");

        return result;
    }

    public String getRootPath() {
        return this.properties.getProperty(keys.rootPath.toString());
    }

    public String getConnectStr() {
        return this.properties.getProperty(keys.zkConnectString.toString());
    }

    public boolean checkZookeeperState() {
        return zk != null && zk.getState() == CuratorFrameworkState.STARTED;
    }
}
