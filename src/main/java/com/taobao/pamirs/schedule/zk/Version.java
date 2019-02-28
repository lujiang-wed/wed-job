package com.taobao.pamirs.schedule.zk;

public class Version {

    public final static String SYS_VERSION = "release-1.0.1";

    public final static String ZK_VERSION = "3.4.9";

    public static String getVersion() {
        return SYS_VERSION + "|" + ZK_VERSION;
    }

    public static boolean isCompatible(String dataVersion) {
        return (SYS_VERSION + "|" + ZK_VERSION).compareTo(dataVersion) >= 0;
    }
}
