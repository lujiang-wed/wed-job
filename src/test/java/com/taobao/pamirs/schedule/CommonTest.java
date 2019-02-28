package com.taobao.pamirs.schedule;

import com.taobao.pamirs.schedule.strategy.TBScheduleManagerFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;


public class CommonTest {
    @Test
    public static void main(String... args) {
        try {
            String zkConnectString = "19.19.22.51:2181,19.19.22.52:2181,19.19.22.53:2181";
            String rootPath = "/wed-job/tasks_center";
            String zkSessionTimeout = "20000";
            String userName = "admin";
            String password = "admin";

            TBScheduleManagerFactory tbScheduleManagerFactory = new TBScheduleManagerFactory();
            Map<String, String> zkConfig = new HashMap<String, String>();
            zkConfig.put("zkConnectString", zkConnectString);
            zkConfig.put("rootPath", rootPath);
            zkConfig.put("zkSessionTimeout", zkSessionTimeout);
            zkConfig.put("userName", userName);
            zkConfig.put("password", password);
            tbScheduleManagerFactory.setZkConfig(zkConfig);

            tbScheduleManagerFactory.init();
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}