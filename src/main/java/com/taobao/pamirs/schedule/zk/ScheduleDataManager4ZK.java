package com.taobao.pamirs.schedule.zk;

import com.google.gson.*;
import com.taobao.pamirs.schedule.ScheduleUtil;
import com.taobao.pamirs.schedule.TaskItemDefine;
import com.taobao.pamirs.schedule.taskmanager.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ScheduleDataManager4ZK implements IScheduleDataManager {
    private static final Logger logger = LoggerFactory.getLogger(ScheduleDataManager4ZK.class);

    private Gson      gson;
    private ZKManager zkManager;
    private String    pathBaseTaskType;
    private long      zkBaseTime;
    private long      loclaBaseTime;

    private final Pattern taskItemExpr  = Pattern.compile("\\s*:\\s*\\{");
    private final String  PATH_TaskItem = "taskItem";
    private final String  PATH_Server   = "server";

    public ScheduleDataManager4ZK(ZKManager aZkManager) throws Exception {
        this.zkManager = aZkManager;
        gson = new GsonBuilder().registerTypeAdapter(Timestamp.class, new TimestampTypeAdapter()).setDateFormat("yyyy-MM-dd HH:mm:ss").create();

        this.pathBaseTaskType = this.zkManager.getDefaultPath() + "/baseTaskType";

        if (this.getZooKeeper().checkExists().forPath(this.pathBaseTaskType) == null) {
            this.getZooKeeper()
                    .create()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(this.zkManager.getAcl())
                    .forPath(this.pathBaseTaskType, null);
        }
        loclaBaseTime = System.currentTimeMillis();
        String tempPath = this.getZooKeeper().create()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .withACL(this.zkManager.getAcl())
                .forPath(this.zkManager.getDefaultPath() + "/systime", null);

        this.getZooKeeper().getConnectionStateListenable().addListener(new SessionConnectionListener(tempPath, "/systime"));

        Stat tempStat = this.getZooKeeper().checkExists().forPath(tempPath);
        zkBaseTime = tempStat.getCtime();
        this.getZooKeeper().delete()
                .deletingChildrenIfNeeded()
                .forPath(tempPath);
        if (Math.abs(this.zkBaseTime - this.loclaBaseTime) > 5000) {
            logger.error("请注意，Zookeeper服务器时间与本地时间相差 ： " + Math.abs(this.zkBaseTime - this.loclaBaseTime) + " ms");
        }
    }

    public CuratorFramework getZooKeeper() {
        return this.zkManager.getZkClient();
    }

    @Override
    public void createBaseTaskType(ScheduleTaskType baseTaskType) throws Exception {
        if (baseTaskType.getBaseTaskType().indexOf("$") > 0) {
            throw new Exception("调度任务" + baseTaskType.getBaseTaskType() + "名称不能包括特殊字符 $");
        }
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType.getBaseTaskType();
        String valueString = this.gson.toJson(baseTaskType);
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            this.getZooKeeper().create()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(this.zkManager.getAcl())
                    .forPath(zkPath, valueString.getBytes());
        } else {
            throw new Exception("调度任务" + baseTaskType.getBaseTaskType() + "已经存在,如果确认需要重建，请先调用deleteTaskType(String baseTaskType)删除");
        }
    }

    @Override
    public void updateBaseTaskType(ScheduleTaskType baseTaskType)
            throws Exception {
        if (baseTaskType.getBaseTaskType().indexOf("$") > 0) {
            throw new Exception("调度任务" + baseTaskType.getBaseTaskType() + "名称不能包括特殊字符 $");
        }
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType.getBaseTaskType();
        String valueString = this.gson.toJson(baseTaskType);
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            this.getZooKeeper().create()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(this.zkManager.getAcl())
                    .forPath(zkPath, valueString.getBytes());
        } else {
            this.getZooKeeper().setData().forPath(zkPath, valueString.getBytes());
        }

    }

    @Override
    public void initialRunningInfo4Dynamic(String baseTaskType, String ownSign) throws Exception {
        String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);
        //清除所有的老信息，只有leader能执行此操作
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType;
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            this.getZooKeeper().create()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(this.zkManager.getAcl())
                    .forPath(zkPath, null);
        }
    }

    @Override
    public void initialRunningInfo4Static(String baseTaskType, String ownSign, String uuid)
            throws Exception {

        String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);
        //清除所有的老信息，只有leader能执行此操作
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        try {
            this.getZooKeeper().delete().deletingChildrenIfNeeded().forPath(zkPath);
        } catch (Exception e) {
            //需要处理zookeeper session过期异常
            if (e instanceof KeeperException
                    && ((KeeperException) e).code().intValue() == KeeperException.Code.SESSIONEXPIRED.intValue()) {
                logger.warn("delete : zookeeper session已经过期，需要重新连接zookeeper");
                zkManager.getZkClient().start();
                this.zkManager.getZkClient().delete().deletingChildrenIfNeeded().forPath(zkPath);
            }
        }
        //创建目录
        this.getZooKeeper().create()
                .withMode(CreateMode.PERSISTENT)
                .withACL(this.zkManager.getAcl())
                .forPath(zkPath, null);
        //创建静态任务
        this.createScheduleTaskItem(baseTaskType, ownSign, this.loadTaskTypeBaseInfo(baseTaskType).getTaskItems());
        //标记信息初始化成功
        setInitialRunningInfoSucuss(baseTaskType, taskType, uuid);
    }

    @Override
    public void setInitialRunningInfoSucuss(String baseTaskType, String taskType, String uuid) throws Exception {
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        this.getZooKeeper().setData()
                .forPath(zkPath, uuid.getBytes());
    }

    @Override
    public boolean isInitialRunningInfoSucuss(String baseTaskType, String ownSign) throws Exception {
        String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);
        String leader = this.getLeader(this.loadScheduleServerNames(taskType));
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        if (this.getZooKeeper().checkExists().forPath(zkPath) != null) {
            byte[] curContent = this.getZooKeeper().getData().forPath(zkPath);
            return curContent != null && new String(curContent).equals(leader);
        }
        return false;
    }

    @Override
    public long updateReloadTaskItemFlag(String taskType) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType
                + "/" + taskType + "/" + this.PATH_Server;
        Stat stat = this.getZooKeeper().setData().forPath(zkPath, "reload=true".getBytes());
        return stat.getVersion();
    }

    @Override
    public long getReloadTaskItemFlag(String taskType) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType
                + "/" + taskType + "/" + this.PATH_Server;
        Stat stat = new Stat();
        this.getZooKeeper().getData().storingStatIn(stat).forPath(zkPath);
        return stat.getVersion();
    }

    @Override
    public Map<String, Stat> getCurrentServerStatList(String taskType) throws Exception {
        Map<String, Stat> statMap = new HashMap<String, Stat>();
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType
                + "/" + taskType + "/" + this.PATH_Server;
        List<String> childs = this.getZooKeeper().getChildren().forPath(zkPath);
        for (String serv : childs) {
            String singleServ = zkPath + "/" + serv;
            Stat servStat = this.getZooKeeper().checkExists().forPath(singleServ);
            statMap.put(serv, servStat);
        }
        return statMap;
    }

    /**
     * 根据基础配置里面的任务项来创建各个域里面的任务项
     *
     * @param baseTaskType
     * @param ownSign
     * @param baseTaskItems
     * @throws Exception
     */
    public void createScheduleTaskItem(String baseTaskType, String ownSign, String[] baseTaskItems) throws Exception {
        ScheduleTaskItem[] taskItems = new ScheduleTaskItem[baseTaskItems.length];
        for (int i = 0; i < baseTaskItems.length; i++) {
            taskItems[i] = new ScheduleTaskItem();
            taskItems[i].setBaseTaskType(baseTaskType);
            taskItems[i].setTaskType(ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign));
            taskItems[i].setOwnSign(ownSign);
            Matcher matcher = taskItemExpr.matcher(baseTaskItems[i]);
            if (matcher.find()) {
                taskItems[i].setTaskItem(baseTaskItems[i].substring(0, matcher.start()).trim());
                taskItems[i].setDealParameter(baseTaskItems[i].substring(matcher.end(), baseTaskItems[i].length() - 1).trim());
            } else {
                taskItems[i].setTaskItem(baseTaskItems[i]);
            }
            taskItems[i].setSts(ScheduleTaskItem.TaskItemSts.ACTIVTE);
        }
        createScheduleTaskItem(taskItems);
    }

    /**
     * 创建任务项，注意其中的 CurrentSever和RequestServer不会起作用
     *
     * @param taskItems
     * @throws Exception
     */
    @Override
    public void createScheduleTaskItem(ScheduleTaskItem[] taskItems) throws Exception {
        for (ScheduleTaskItem taskItem : taskItems) {
            String zkPath = this.pathBaseTaskType + "/" + taskItem.getBaseTaskType() + "/" + taskItem.getTaskType() + "/" + this.PATH_TaskItem;
            if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
                this.getZooKeeper().create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .withACL(this.zkManager.getAcl())
                        .forPath(zkPath, null);
            }
            String zkTaskItemPath = zkPath + "/" + taskItem.getTaskItem();
            this.getZooKeeper().create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(this.zkManager.getAcl())
                    .forPath(zkTaskItemPath, null);
            this.getZooKeeper().create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(this.zkManager.getAcl())
                    .forPath(zkTaskItemPath + "/cur_server", null);
            this.getZooKeeper().create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(this.zkManager.getAcl())
                    .forPath(zkTaskItemPath + "/req_server", null);
            this.getZooKeeper().create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(this.zkManager.getAcl())
                    .forPath(zkTaskItemPath + "/sts", taskItem.getSts().toString().getBytes());
            this.getZooKeeper().create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(this.zkManager.getAcl())
                    .forPath(zkTaskItemPath + "/parameter", taskItem.getDealParameter().getBytes());
            this.getZooKeeper().create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(this.zkManager.getAcl())
                    .forPath(zkTaskItemPath + "/deal_desc", taskItem.getDealDesc().getBytes());
        }
    }

    @Override
    public void updateScheduleTaskItemStatus(String taskType, String taskItem, ScheduleTaskItem.TaskItemSts sts, String message) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem + "/" + taskItem;
        if (this.getZooKeeper().checkExists().forPath(zkPath + "/sts") == null) {
            this.getZooKeeper().setData()
                    .forPath(zkPath + "/sts", sts.toString().getBytes());
        }
        if (this.getZooKeeper().checkExists().forPath(zkPath + "/deal_desc") == null) {
            if (message == null) {
                message = "";
            }
            this.getZooKeeper().setData().forPath(zkPath + "/deal_desc", message.getBytes());
        }
    }

    /**
     * 删除任务项
     *
     * @param taskType
     * @param taskItem
     * @throws Exception
     */
    @Override
    public void deleteScheduleTaskItem(String taskType, String taskItem) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem + "/" + taskItem;
        this.getZooKeeper().delete()
                .deletingChildrenIfNeeded()
                .forPath(zkPath);
    }

    @Override
    public List<ScheduleTaskItem> loadAllTaskItem(String taskType) throws Exception {
        List<ScheduleTaskItem> result = new ArrayList<ScheduleTaskItem>();
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            return result;
        }
        List<String> taskItems = this.getZooKeeper().getChildren().forPath(zkPath);
        Collections.sort(taskItems);
        for (String taskItem : taskItems) {
            ScheduleTaskItem info = new ScheduleTaskItem();
            info.setTaskType(taskType);
            info.setTaskItem(taskItem);
            String zkTaskItemPath = zkPath + "/" + taskItem;
            byte[] curContent = this.getZooKeeper().getData().forPath(zkTaskItemPath + "/cur_server");
            if (curContent != null) {
                info.setCurrentScheduleServer(new String(curContent));
            }
            byte[] reqContent = this.getZooKeeper().getData().forPath(zkTaskItemPath + "/req_server");
            if (reqContent != null) {
                info.setRequestScheduleServer(new String(reqContent));
            }
            byte[] stsContent = this.getZooKeeper().getData().forPath(zkTaskItemPath + "/sts");
            if (stsContent != null) {
                info.setSts(ScheduleTaskItem.TaskItemSts.valueOf(new String(stsContent)));
            }
            byte[] parameterContent = this.getZooKeeper().getData().forPath(zkTaskItemPath + "/parameter");
            if (parameterContent != null) {
                info.setDealParameter(new String(parameterContent));
            }
            byte[] dealDescContent = this.getZooKeeper().getData().forPath(zkTaskItemPath + "/deal_desc");
            if (dealDescContent != null) {
                info.setDealDesc(new String(dealDescContent));
            }
            result.add(info);
        }
        return result;

    }

    @Override
    public ScheduleTaskType loadTaskTypeBaseInfo(String baseTaskType) throws Exception {
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType;
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            return null;
        }
        String valueString = new String(this.getZooKeeper().getData().forPath(zkPath));
        return (ScheduleTaskType) this.gson.fromJson(valueString, ScheduleTaskType.class);
    }

    @Override
    public List<ScheduleTaskType> getAllTaskTypeBaseInfo() throws Exception {
        String zkPath = this.pathBaseTaskType;
        List<ScheduleTaskType> result = new ArrayList<ScheduleTaskType>();
        List<String> names = this.getZooKeeper().getChildren().forPath(zkPath);
        Collections.sort(names);
        for (String name : names) {
            result.add(this.loadTaskTypeBaseInfo(name));
        }
        return result;
    }

    @Override
    public void clearTaskType(String baseTaskType) throws Exception {
        //清除所有的Runtime TaskType
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType;
        List<String> list = this.getZooKeeper().getChildren().forPath(zkPath);
        for (String name : list) {
            this.getZooKeeper().delete().deletingChildrenIfNeeded()
                    .forPath(zkPath + "/" + name);
        }
    }

    @Override
    public List<ScheduleTaskTypeRunningInfo> getAllTaskTypeRunningInfo(
            String baseTaskType) throws Exception {
        List<ScheduleTaskTypeRunningInfo> result = new ArrayList<ScheduleTaskTypeRunningInfo>();
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType;
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            return result;
        }
        List<String> list = this.getZooKeeper().getChildren().forPath(zkPath);
        Collections.sort(list);

        for (String name : list) {
            ScheduleTaskTypeRunningInfo info = new ScheduleTaskTypeRunningInfo();
            info.setBaseTaskType(baseTaskType);
            info.setTaskType(name);
            info.setOwnSign(ScheduleUtil.splitOwnsignFromTaskType(name));
            result.add(info);
        }
        return result;
    }

    @Override
    public void deleteTaskType(String baseTaskType) throws Exception {
        this.getZooKeeper().delete()
                .deletingChildrenIfNeeded().forPath(this.pathBaseTaskType + "/" + baseTaskType);
    }

    @Override
    public List<ScheduleServer> selectScheduleServer(String baseTaskType,
                                                     String ownSign, String ip, String orderStr) throws Exception {
        List<String> names = new ArrayList<String>();
        if (baseTaskType != null && ownSign != null) {
            names.add(baseTaskType + "$" + ownSign);
        } else if (baseTaskType != null) {
            if (this.getZooKeeper().checkExists().forPath(this.pathBaseTaskType + "/" + baseTaskType) != null) {
                names.addAll(this.getZooKeeper().getChildren().forPath(this.pathBaseTaskType + "/" + baseTaskType));
            }
        } else {
            for (String name : this.getZooKeeper().getChildren().forPath(this.pathBaseTaskType)) {
                if (ownSign != null) {
                    names.add(name + "$" + ownSign);
                } else {
                    names.addAll(this.getZooKeeper().getChildren().forPath(this.pathBaseTaskType + "/" + name));
                }
            }
        }
        List<ScheduleServer> result = new ArrayList<ScheduleServer>();
        for (String name : names) {
            List<ScheduleServer> tempList = this.selectAllValidScheduleServer(name);
            if (ip == null) {
                result.addAll(tempList);
            } else {
                for (ScheduleServer server : tempList) {
                    if (ip.equals(server.getIp())) {
                        result.add(server);
                    }
                }
            }
        }
        result.sort(new ScheduleServerComparator(orderStr));
        //排序
        return result;
    }

    @Override
    public List<ScheduleServer> selectHistoryScheduleServer(
            String baseTaskType, String ownSign, String ip, String orderStr)
            throws Exception {
        throw new Exception("没有实现的方法");
    }

    @Override
    public List<TaskItemDefine> reloadDealTaskItem(String taskType, String uuid)
            throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;

        List<String> taskItems = this.getZooKeeper().getChildren().forPath(zkPath);
        Collections.sort(taskItems);

        List<TaskItemDefine> result = new ArrayList<TaskItemDefine>();
        for (String name : taskItems) {
            byte[] value = this.getZooKeeper().getData().forPath(zkPath + "/" + name + "/cur_server");
            if (value != null && uuid.equals(new String(value))) {
                TaskItemDefine item = new TaskItemDefine();
                item.setTaskItemId(name);
                byte[] parameterValue = this.getZooKeeper().getData().forPath(zkPath + "/" + name + "/parameter");
                if (parameterValue != null) {
                    item.setParameter(new String(parameterValue));
                }
                result.add(item);
            }
        }
        return result;
    }

    @Override
    public void releaseDealTaskItem(String taskType, String uuid) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        boolean isModify = false;
        for (String name : this.getZooKeeper().getChildren().forPath(zkPath)) {
            byte[] curServerValue = this.getZooKeeper().getData().forPath(zkPath + "/" + name + "/cur_server");
            byte[] reqServerValue = this.getZooKeeper().getData().forPath(zkPath + "/" + name + "/req_server");
            if (reqServerValue != null && curServerValue != null && uuid.equals(new String(curServerValue))) {
                this.getZooKeeper().setData().forPath(zkPath + "/" + name + "/cur_server", reqServerValue);
                this.getZooKeeper().setData().forPath(zkPath + "/" + name + "/req_server", null);
                isModify = true;
            }
        }
        if (isModify) {
            //设置需要所有的服务器重新装载任务
            this.updateReloadTaskItemFlag(taskType);
        }
    }

    @Override
    public int queryTaskItemCount(String taskType) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        return this.getZooKeeper().getChildren().forPath(zkPath).size();
    }

    @Override
    public void clearExpireTaskTypeRunningInfo(String baseTaskType, String serverUUID, double expireDateInternal) throws Exception {
        for (String name : this.getZooKeeper().getChildren().forPath(this.pathBaseTaskType + "/" + baseTaskType)) {
            String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + name + "/" + this.PATH_TaskItem;
            Stat stat = this.getZooKeeper().checkExists().forPath(zkPath);
            if (stat == null || getSystemTime() - stat.getMtime() > (long) (expireDateInternal * 24 * 3600 * 1000)) {
                this.getZooKeeper().delete()
                        .deletingChildrenIfNeeded().forPath(this.pathBaseTaskType + "/" + baseTaskType + "/" + name);
            }
        }
    }

    @Override
    public int clearExpireScheduleServer(String taskType, long expireTime) throws Exception {
        int result = 0;
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType
                + "/" + taskType + "/" + this.PATH_Server;
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            String tempPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType;
            if (this.getZooKeeper().checkExists().forPath(tempPath) == null) {
                this.getZooKeeper().create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .withACL(this.zkManager.getAcl())
                        .forPath(tempPath, null);
            }
            this.getZooKeeper().create().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT).withACL(this.zkManager.getAcl())
                    .forPath(zkPath, null);
        }
        for (String name : this.getZooKeeper().getChildren().forPath(zkPath)) {
            try {
                Stat stat = this.getZooKeeper().checkExists().forPath(zkPath + "/" + name);
                if (getSystemTime() - stat.getMtime() > expireTime) {
                    this.getZooKeeper().delete().deletingChildrenIfNeeded().forPath(zkPath + "/" + name);
                    result++;
                }
            } catch (Exception e) {
                // 当有多台服务器时，存在并发清理的可能，忽略异常
                result++;
            }
        }
        return result;
    }

    @Override
    public int clearTaskItem(String taskType,
                             List<String> serverList) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;

        int result = 0;
        for (String name : this.getZooKeeper().getChildren().forPath(zkPath)) {
            byte[] curServerValue = this.getZooKeeper().getData().forPath(zkPath + "/" + name + "/cur_server");
            if (curServerValue != null) {
                String curServer = new String(curServerValue);
                boolean isFind = false;
                for (String server : serverList) {
                    if (curServer.equals(server)) {
                        isFind = true;
                        break;
                    }
                }
                if (!isFind) {
                    this.getZooKeeper().setData().forPath(zkPath + "/" + name + "/cur_server", null);
                    result = result + 1;
                }
            } else {
                result = result + 1;
            }
        }
        return result;
    }

    @Override
    public List<String> loadScheduleServerNames(String taskType) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            return new ArrayList<String>();
        }
        List<String> serverList = this.getZooKeeper().getChildren().forPath(zkPath);
        serverList.sort(new Comparator<String>() {
            @Override
            public int compare(String u1, String u2) {
                return u1.substring(u1.lastIndexOf("$") + 1).compareTo(
                        u2.substring(u2.lastIndexOf("$") + 1));
            }
        });
        return serverList;
    }

    @Override
    public List<ScheduleServer> selectAllValidScheduleServer(String taskType)
            throws Exception {
        List<ScheduleServer> result = new ArrayList<ScheduleServer>();
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            return result;
        }
        List<String> serverList = this.getZooKeeper().getChildren().forPath(zkPath);
        serverList.sort(Comparator.comparing(u -> u.substring(u.lastIndexOf("$") + 1)));
        for (String name : serverList) {
            try {
                String valueString = new String(this.getZooKeeper().getData().forPath(zkPath + "/" + name));
                ScheduleServer server = this.gson.fromJson(valueString, ScheduleServer.class);
                server.setCenterServerTime(new Timestamp(this.getSystemTime()));
                result.add(server);
            } catch (Exception e) {
                logger.debug(e.getMessage(), e);
            }
        }
        return result;
    }

    @Override
    public List<ScheduleServer> selectScheduleServerByManagerFactoryUUID(String factoryUUID)
            throws Exception {
        List<ScheduleServer> result = new ArrayList<ScheduleServer>();
        for (String baseTaskType : this.getZooKeeper().getChildren().forPath(this.pathBaseTaskType)) {
            for (String taskType : this.getZooKeeper().getChildren().forPath(this.pathBaseTaskType + "/" + baseTaskType)) {
                String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
                for (String uuid : this.getZooKeeper().getChildren().forPath(zkPath)) {
                    String valueString = new String(this.getZooKeeper().getData().forPath(zkPath + "/" + uuid));
                    ScheduleServer server = this.gson.fromJson(valueString, ScheduleServer.class);
                    server.setCenterServerTime(new Timestamp(this.getSystemTime()));
                    if (server.getManagerFactoryUUID().equals(factoryUUID)) {
                        result.add(server);
                    }
                }
            }
        }
        result.sort((u1, u2) -> {
            int result1 = u1.getTaskType().compareTo(u2.getTaskType());
            if (result1 == 0) {
                String s1 = u1.getUuid();
                String s2 = u2.getUuid();
                result1 = s1.substring(s1.lastIndexOf("$") + 1).compareTo(
                        s2.substring(s2.lastIndexOf("$") + 1));
            }
            return result1;
        });
        return result;
    }

    @Override
    public String getLeader(List<String> serverList) {
        if (serverList == null || serverList.size() == 0) {
            return "";
        }
        long no = Long.MAX_VALUE;
        long tmpNo = -1;
        String leader = null;
        for (String server : serverList) {
            tmpNo = Long.parseLong(server.substring(server.lastIndexOf("$") + 1));
            if (no > tmpNo) {
                no = tmpNo;
                leader = server;
            }
        }
        return leader;
    }

    @Override
    public boolean isLeader(String uuid, List<String> serverList) {
        return uuid.equals(getLeader(serverList));
    }

    @Override
    public void assignTaskItem(String taskType, String currentUuid, int maxNumOfOneServer,
                               List<String> taskServerList) throws Exception {
        if (!this.isLeader(currentUuid, taskServerList)) {
            if (logger.isDebugEnabled()) {
                logger.debug(currentUuid + ":不是负责任务分配的Leader,直接返回");
            }
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug(currentUuid + ":开始重新分配任务......");
        }
        if (taskServerList.size() <= 0) {
            //在服务器动态调整的时候，可能出现服务器列表为空的清空
            return;
        }
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        List<String> children = this.getZooKeeper().getChildren().forPath(zkPath);
        Collections.sort(children);
        int unModifyCount = 0;
        int[] taskNums = ScheduleUtil.assignTaskNumber(taskServerList.size(), children.size(), maxNumOfOneServer);
        int point = 0;
        int count = 0;
        String NO_SERVER_DEAL = "没有分配到服务器";
        for (int i = 0; i < children.size(); i++) {
            String name = children.get(i);
            if (point < taskServerList.size() && i >= count + taskNums[point]) {
                count = count + taskNums[point];
                point = point + 1;
            }
            String serverName = NO_SERVER_DEAL;
            if (point < taskServerList.size()) {
                serverName = taskServerList.get(point);
            }
            byte[] curServerValue = this.getZooKeeper().getData().forPath(zkPath + "/" + name + "/cur_server");
            byte[] reqServerValue = this.getZooKeeper().getData().forPath(zkPath + "/" + name + "/req_server");

            if (curServerValue == null || new String(curServerValue).equals(NO_SERVER_DEAL)) {
                this.getZooKeeper().setData().forPath(zkPath + "/" + name + "/cur_server", serverName.getBytes());
                this.getZooKeeper().setData().forPath(zkPath + "/" + name + "/req_server", null);
            } else if (new String(curServerValue).equals(serverName) && reqServerValue == null) {
                //不需要做任何事情
                unModifyCount = unModifyCount + 1;
            } else {
                this.getZooKeeper().setData().forPath(zkPath + "/" + name + "/req_server", serverName.getBytes());
            }
        }

        if (unModifyCount < children.size()) { //设置需要所有的服务器重新装载任务
            this.updateReloadTaskItemFlag(taskType);
        }
        if (logger.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            for (ScheduleTaskItem taskItem : this.loadAllTaskItem(taskType)) {
                buffer.append("\n").append(taskItem.toString());
            }
            logger.debug(buffer.toString());
        }
    }

    public void assignTaskItem22(String taskType, String currentUuid,
                                 List<String> serverList) throws Exception {
        if (!this.isLeader(currentUuid, serverList)) {
            if (logger.isDebugEnabled()) {
                logger.debug(currentUuid + ":不是负责任务分配的Leader,直接返回");
            }
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug(currentUuid + ":开始重新分配任务......");
        }
        if (serverList.size() <= 0) {
            //在服务器动态调整的时候，可能出现服务器列表为空的清空
            return;
        }
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        int point = 0;
        List<String> children = this.getZooKeeper().getChildren().forPath(zkPath);
        Collections.sort(children);
        int unModifyCount = 0;
        for (String name : children) {
            byte[] curServerValue = this.getZooKeeper().getData().forPath(zkPath + "/" + name + "/cur_server");
            byte[] reqServerValue = this.getZooKeeper().getData().forPath(zkPath + "/" + name + "/req_server");
            if (curServerValue == null) {
                this.getZooKeeper().setData().forPath(zkPath + "/" + name + "/cur_server", serverList.get(point).getBytes());
                this.getZooKeeper().setData().forPath(zkPath + "/" + name + "/req_server", null);
            } else if (new String(curServerValue).equals(serverList.get(point)) && reqServerValue == null) {
                //不需要做任何事情
                unModifyCount = unModifyCount + 1;
            } else {
                this.getZooKeeper().setData().forPath(zkPath + "/" + name + "/req_server", serverList.get(point).getBytes());
            }
            point = (point + 1) % serverList.size();
        }

        if (unModifyCount < children.size()) { //设置需要所有的服务器重新装载任务
            this.updateReloadTaskItemFlag(taskType);
        }
        if (logger.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            for (ScheduleTaskItem taskItem : this.loadAllTaskItem(taskType)) {
                buffer.append("\n").append(taskItem.toString());
            }
            logger.debug(buffer.toString());
        }
    }

    @Override
    public void registerScheduleServer(ScheduleServer server) throws Exception {
        if (server.isRegister()) {
            throw new Exception(server.getUuid() + " 被重复注册");
        }
        String zkPath = this.pathBaseTaskType + "/" + server.getBaseTaskType() + "/" + server.getTaskType();
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            this.getZooKeeper()
                    .create()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(this.zkManager.getAcl())
                    .forPath(zkPath, null);
        }
        zkPath = zkPath + "/" + this.PATH_Server;
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            this.getZooKeeper()
                    .create()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(this.zkManager.getAcl())
                    .forPath(zkPath, null);
        }
        String realPath = null;
        //此处必须增加UUID作为唯一性保障
        String zkServerPath = zkPath + "/" + server.getTaskType() + "$" + server.getIp() + "$"
                + (UUID.randomUUID().toString().replaceAll("-", "").toUpperCase()) + "$";

        realPath = this.getZooKeeper()
                .create()
                .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                .withACL(this.zkManager.getAcl())
                .forPath(zkServerPath, null);

        server.setUuid(realPath.substring(realPath.lastIndexOf("/") + 1));

        Timestamp heartBeatTime = new Timestamp(this.getSystemTime());
        server.setHeartBeatTime(heartBeatTime);

        String valueString = this.gson.toJson(server);
        this.getZooKeeper().setData().forPath(realPath, valueString.getBytes());
        server.setRegister(true);
    }

    @Override
    public boolean refreshScheduleServer(ScheduleServer server) throws Exception {
        Timestamp heartBeatTime = new Timestamp(this.getSystemTime());
        String zkPath = this.pathBaseTaskType + "/" + server.getBaseTaskType() + "/" + server.getTaskType()
                + "/" + this.PATH_Server + "/" + server.getUuid();
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            //数据可能被清除，先清除内存数据后，重新注册数据
            server.setRegister(false);
            return false;
        } else {
            Timestamp oldHeartBeatTime = server.getHeartBeatTime();
            server.setHeartBeatTime(heartBeatTime);
            server.setVersion(server.getVersion() + 1);
            String valueString = this.gson.toJson(server);
            try {
                this.getZooKeeper().setData().forPath(zkPath, valueString.getBytes());
            } catch (Exception e) {
                //恢复上次的心跳时间
                server.setHeartBeatTime(oldHeartBeatTime);
                server.setVersion(server.getVersion() - 1);
                throw e;
            }
            return true;
        }
    }

    @Override
    public void unRegisterScheduleServer(String taskType, String serverUUID) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.pathBaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server + "/" + serverUUID;
        if (this.getZooKeeper().checkExists().forPath(zkPath) != null) {
            this.getZooKeeper().delete().forPath(zkPath);
        }
    }

    @Override
    public void pauseAllServer(String baseTaskType) throws Exception {
        ScheduleTaskType taskType = this.loadTaskTypeBaseInfo(baseTaskType);
        taskType.setSts(ScheduleTaskType.STS_PAUSE);
        this.updateBaseTaskType(taskType);
    }

    @Override
    public void resumeAllServer(String baseTaskType) throws Exception {
        ScheduleTaskType taskType = this.loadTaskTypeBaseInfo(baseTaskType);
        taskType.setSts(ScheduleTaskType.STS_RESUME);
        this.updateBaseTaskType(taskType);
    }

    @Override
    public long getSystemTime() {
        return this.zkBaseTime + (System.currentTimeMillis() - this.loclaBaseTime);
    }

}

class ScheduleServerComparator implements Comparator<ScheduleServer> {
    String[] orderFields;

    public ScheduleServerComparator(String aOrderStr) {
        if (aOrderStr != null) {
            orderFields = aOrderStr.toUpperCase().split(",");
        } else {
            orderFields = "TASK_TYPE,OWN_SIGN,REGISTER_TIME,HEARTBEAT_TIME,IP".toUpperCase().split(",");
        }
    }

    public int compareObject(String o1, String o2) {
        if (o1 == null && o2 == null) {
            return 0;
        } else if (o1 != null) {
            return o1.compareTo(o2);
        } else {
            return -1;
        }
    }

    public int compareObject(Timestamp o1, Timestamp o2) {
        if (o1 == null && o2 == null) {
            return 0;
        } else if (o1 != null) {
            return o1.compareTo(o2);
        } else {
            return -1;
        }
    }

    @Override
    public int compare(ScheduleServer o1, ScheduleServer o2) {
        int result = 0;
        for (String name : orderFields) {
            if (name.equals("TASK_TYPE")) {
                result = compareObject(o1.getTaskType(), o2.getTaskType());
                if (result != 0) {
                    return result;
                }
            } else if (name.equals("OWN_SIGN")) {
                result = compareObject(o1.getOwnSign(), o2.getOwnSign());
                if (result != 0) {
                    return result;
                }
            } else if (name.equals("REGISTER_TIME")) {
                result = compareObject(o1.getRegisterTime(), o2.getRegisterTime());
                if (result != 0) {
                    return result;
                }
            } else if (name.equals("HEARTBEAT_TIME")) {
                result = compareObject(o1.getHeartBeatTime(), o2.getHeartBeatTime());
                if (result != 0) {
                    return result;
                }
            } else if (name.equals("IP")) {
                result = compareObject(o1.getIp(), o2.getIp());
                if (result != 0) {
                    return result;
                }
            } else if (name.equals("MANAGER_FACTORY")) {
                result = compareObject(o1.getManagerFactoryUUID(), o2.getManagerFactoryUUID());
                if (result != 0) {
                    return result;
                }
            }
        }
        return result;
    }
}

class TimestampTypeAdapter implements JsonSerializer<Timestamp>, JsonDeserializer<Timestamp> {

    @Override
    public JsonElement serialize(Timestamp src, Type arg1, JsonSerializationContext arg2) {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateFormatAsString = format.format(new Date(src.getTime()));
        return new JsonPrimitive(dateFormatAsString);
    }

    @Override
    public Timestamp deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        if (!(json instanceof JsonPrimitive)) {
            throw new JsonParseException("The date should be a string value");
        }

        try {
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = (Date) format.parse(json.getAsString());
            return new Timestamp(date.getTime());
        } catch (Exception e) {
            throw new JsonParseException(e);
        }
    }
}



 
