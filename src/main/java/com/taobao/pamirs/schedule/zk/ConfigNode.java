package com.taobao.pamirs.schedule.zk;

/**
 * 配置信息
 *
 * @author gjavac@gmail.com
 * @version 1.0
 * @since 2012-2-12
 */
public class ConfigNode {

    private String rootPath;

    private String configType;

    private String name;

    private String value;

    public ConfigNode() {

    }

    public ConfigNode(String rootPath, String configType, String name) {
        this.rootPath = rootPath;
        this.configType = configType;
        this.name = name;
    }

    public String getRootPath() {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    public String getConfigType() {
        return configType;
    }

    public void setConfigType(String configType) {
        this.configType = configType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "配置根目录：" + rootPath + "\n" +
                "配置类型：" + configType + "\n" +
                "任务名称：" + name + "\n" +
                "配置的值：" + value + "\n";
    }
}
