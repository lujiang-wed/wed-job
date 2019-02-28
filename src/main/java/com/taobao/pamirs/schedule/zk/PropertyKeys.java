package com.taobao.pamirs.schedule.zk;

public enum PropertyKeys {
    //连接串ip1:port1,ip2:port2格式
    ZK_CONNECT_STRING("zkConnectString"),
    //根目录
    ROOT_PATH("rootPath"),
    //ACL用户名
    USER_NAME("userName"),
    //ACL密码
    PASSWORD("password"),
    //链接最大超时时间
    ZK_SESSIONT_IMEOUT("zkSessionTimeout");

    private String key;

    PropertyKeys(String key){
        this.key = key;
    }

    public static String getByName(PropertyKeys key){
        return key.getKey();
    }

    public String getKey() {
        return key;
    }
}
