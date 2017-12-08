package com.taobao.pamirs.schedule;

import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class CommonTest {
    @Test
    public static void main(String... args){
        String result = "10.0.10.169"
                + "$"
                + (UUID.randomUUID().toString().replaceAll("-", "")
                .toUpperCase());
        SimpleDateFormat DATA_FORMAT_yyyyMMdd = new SimpleDateFormat("yyMMdd");
        String s = DATA_FORMAT_yyyyMMdd.format(new Date());
        long result1 = Long.parseLong(s) * 100000000
                + Math.abs(result.hashCode() % 100000000);
        System.out.println(result);
        System.out.println(result1);
    }
}
