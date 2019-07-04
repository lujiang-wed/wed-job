package com.taobao.pamirs.schedule.test;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class CommonTest {

    public static void main(String... args) {

        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(2);

        timer.scheduleWithFixedDelay(() -> System.out.println("123"), 2000, 2000, TimeUnit.MILLISECONDS);

        ApplicationContext context = new ClassPathXmlApplicationContext("application-context.xml");
        while (true){
        }
    }
}
