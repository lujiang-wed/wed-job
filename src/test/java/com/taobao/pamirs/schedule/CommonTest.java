package com.taobao.pamirs.schedule;

import com.taobao.pamirs.schedule.strategy.TBScheduleManagerFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;


public class CommonTest {
    @Test
    public static void main(String... args) {
        try {
            String zkConnectString = "172.16.60.12:2181,172.16.60.16:2182,172.16.60.33:2183";
            String rootPath = "/rrkd_java_schedule/tasks_center";
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