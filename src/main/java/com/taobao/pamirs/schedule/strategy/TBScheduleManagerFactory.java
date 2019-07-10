package com.taobao.pamirs.schedule.strategy;

import com.taobao.pamirs.schedule.ConsoleManager;
import com.taobao.pamirs.schedule.IScheduleTaskDeal;
import com.taobao.pamirs.schedule.ScheduleUtil;
import com.taobao.pamirs.schedule.taskmanager.IScheduleDataManager;
import com.taobao.pamirs.schedule.taskmanager.TBScheduleManagerStatic;
import com.taobao.pamirs.schedule.zk.ScheduleDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ScheduleStrategyDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ZKManager;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TBScheduleManagerFactory implements ApplicationContextAware {

    private static final Logger  logger               = LoggerFactory.getLogger(TBScheduleManagerFactory.class);
    public               boolean start                = true;
    public volatile      long    timerTaskHeartBeatTS = System.currentTimeMillis();
    protected ZKManager zkManager;
    protected Lock   lock         = new ReentrantLock();
    volatile  String errorMessage = "No config Zookeeper connect infomation";
    private Map<String, String> zkConfig;
    private int timerInterval = 2000;
    private IScheduleDataManager           scheduleDataManager;
    private ScheduleStrategyDataManager4ZK scheduleStrategyManager;
    private Map<String, List<IStrategyTask>> managerMap = new ConcurrentHashMap<>();
    private ApplicationContext          applicationContext;
    private String                      uuid;
    private String                      ip;
    private String                      hostName;
    private ScheduledThreadPoolExecutor timer;
    private ManagerFactoryTimerTask     timerTask;
    private InitialThread               initialThread;

    public TBScheduleManagerFactory() {
        this.ip = ScheduleUtil.getLocalIP();
        this.hostName = ScheduleUtil.getLocalHostName();
    }

    public void init() throws Exception {
        Properties properties = new Properties();
        for (Map.Entry<String, String> e : this.zkConfig.entrySet()) {
            properties.put(e.getKey(), e.getValue());
        }
        this.init(properties);
    }

    public void reInit(Properties p) throws Exception {
        if (this.start || this.timer != null || this.managerMap.size() > 0) {
            throw new Exception("调度器有任务处理，不能重新初始化");
        }
        this.init(p);
    }

    public void init(Properties p) throws Exception {
        if (this.initialThread != null) {
            this.initialThread.stopThread();
        }
        this.lock.lock();
        try {
            this.scheduleDataManager = null;
            this.scheduleStrategyManager = null;
            ConsoleManager.setScheduleManagerFactory(this);
            if (this.zkManager != null) {
                this.zkManager.close();
            }
            this.zkManager = new ZKManager(p);
            this.errorMessage = "Zookeeper connecting ......" + this.zkManager.getConnectStr();
            initialThread = new InitialThread(this);
            initialThread.setName("TBScheduleManagerFactory-initialThread");
            initialThread.start();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 在Zk状态正常后回调数据初始化
     *
     * @throws Exception
     */
    public void initialData() throws Exception {
        this.scheduleDataManager = new ScheduleDataManager4ZK(this.zkManager);
        this.scheduleStrategyManager = new ScheduleStrategyDataManager4ZK(this.zkManager);
        if (this.start) {
            // 注册调度管理器
            this.scheduleStrategyManager.registerManagerFactory(this);
            if (timer == null) {
                timer = new ScheduledThreadPoolExecutor(2);
            }
            if (timerTask == null) {
                timerTask = new ManagerFactoryTimerTask(this);
                timer.schedule(timerTask, this.timerInterval, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * 创建调度服务器
     *
     * @param strategy
     * @throws Exception
     */
    public IStrategyTask createStrategyTask(ScheduleStrategy strategy) {
        IStrategyTask result = null;
        try {
            if (ScheduleStrategy.Kind.Schedule == strategy.getKind()) {
                String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(strategy.getTaskName());
                String ownSign = ScheduleUtil.splitOwnsignFromTaskType(strategy.getTaskName());
                result = new TBScheduleManagerStatic(this, baseTaskType, ownSign, scheduleDataManager);
            } else if (ScheduleStrategy.Kind.Java == strategy.getKind()) {
                result = (IStrategyTask) Class.forName(strategy.getTaskName()).newInstance();
                result.initialTaskParameter(strategy.getStrategyName(), strategy.getTaskParameter());
            } else if (ScheduleStrategy.Kind.Bean == strategy.getKind()) {
                result = (IStrategyTask) this.getBean(strategy.getTaskName());
                result.initialTaskParameter(strategy.getStrategyName(), strategy.getTaskParameter());
            }
        } catch (Exception ex) {
            logger.error("strategy 获取对应的java or bean 出错,schedule并没有加载该任务,请确认" + strategy.getStrategyName(), ex);
        }
        return result;
    }

    public void refresh() throws Exception {
        this.lock.lock();
        try {
            // 判断状态是否终止
            ManagerFactoryInfo stsInfo = null;
            boolean isException = false;
            try {
                stsInfo = this.getScheduleStrategyManager().loadManagerFactoryInfo(this.getUuid());
            } catch (Exception e) {
                isException = true;
                logger.error("获取服务器信息有误：uuid=" + this.getUuid(), e);
            }
            if (isException) {
                try {
                    // 停止所有的调度任务
                    stopServer(null);
                    this.getScheduleStrategyManager().unRregisterManagerFactory(this);
                } finally {
                    reRegisterManagerFactory();
                }
            } else if (!stsInfo.isStart()) {
                // 停止所有的调度任务
                stopServer(null);
                this.getScheduleStrategyManager().unRregisterManagerFactory(
                        this);
            } else {
                reRegisterManagerFactory();
            }
        } finally {
            this.lock.unlock();
        }
    }

    public void reRegisterManagerFactory() throws Exception {
        //重新分配调度器
        List<String> stopList = this.getScheduleStrategyManager().registerManagerFactory(this);
        for (String strategyName : stopList) {
            this.stopServer(strategyName);
        }
        this.assignScheduleServer();
        this.reRunScheduleServer();
    }

    /**
     * 根据策略重新分配调度任务的机器
     *
     * @throws Exception
     */
    public void assignScheduleServer() throws Exception {
        for (ScheduleStrategyRunntime run : this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(this.uuid)) {
            List<ScheduleStrategyRunntime> factoryList = this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByTaskType(run.getStrategyName());
            if (factoryList.size() == 0 || this.isLeader(this.uuid, factoryList) == false) {
                continue;
            }
            ScheduleStrategy scheduleStrategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());

            int[] nums = ScheduleUtil.assignTaskNumber(factoryList.size(), scheduleStrategy.getAssignNum(), scheduleStrategy.getNumOfSingleServer());
            for (int i = 0; i < factoryList.size(); i++) {
                ScheduleStrategyRunntime factory = factoryList.get(i);
                //更新请求的服务器数量
                this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(run.getStrategyName(),
                        factory.getUuid(), nums[i]);
            }
        }
    }

    public boolean isLeader(String uuid, List<ScheduleStrategyRunntime> factoryList) {
        try {
            long no = Long.parseLong(uuid.substring(uuid.lastIndexOf("$") + 1));
            for (ScheduleStrategyRunntime server : factoryList) {
                if (no > Long.parseLong(server.getUuid().substring(
                        server.getUuid().lastIndexOf("$") + 1))) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            logger.error("判断Leader出错：uuif=" + uuid, e);
            return true;
        }
    }

    public void reRunScheduleServer() throws Exception {
        for (ScheduleStrategyRunntime run : this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(this.uuid)) {
            List<IStrategyTask> list = this.managerMap.computeIfAbsent(run.getStrategyName(), k -> new ArrayList<>());
            while (list.size() > run.getRequestNum() && list.size() > 0) {
                IStrategyTask task = list.remove(list.size() - 1);
                try {
                    task.stop(run.getStrategyName());
                } catch (Throwable e) {
                    logger.error("注销任务错误：strategyName=" + run.getStrategyName(), e);
                }
            }
            //不足，增加调度器
            ScheduleStrategy strategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
            while (list.size() < run.getRequestNum()) {
                IStrategyTask result = this.createStrategyTask(strategy);
                if (null == result) {
                    logger.error("strategy 对应的配置有问题。strategy name=" + strategy.getStrategyName());
                } else {
                    list.add(result);
                }
            }
        }
    }

    /**
     * 终止一类任务
     *
     * @param strategyName
     * @throws Exception
     */
    public void stopServer(String strategyName) throws Exception {
        if (strategyName == null) {
            String[] nameList = this.managerMap.keySet().toArray(new String[0]);
            for (String name : nameList) {
                for (IStrategyTask task : this.managerMap.get(name)) {
                    try {
                        //TODO:TEST
                        task.stop(strategyName);
                    } catch (Throwable e) {
                        logger.error("注销任务错误：strategyName=" + strategyName, e);
                    }
                }
                this.managerMap.remove(name);
            }
        } else {
            List<IStrategyTask> list = this.managerMap.get(strategyName);
            if (list != null) {
                for (IStrategyTask task : list) {
                    try {
                        task.stop(strategyName);
                    } catch (Throwable e) {
                        logger.error("注销任务错误：strategyName=" + strategyName, e);
                    }
                }
                this.managerMap.remove(strategyName);
            }

        }
    }

    /**
     * 停止所有调度资源
     */
    public void stopAll() throws Exception {
        try {
            lock.lock();
            this.start = false;
            if (this.initialThread != null) {
                this.initialThread.stopThread();
            }
            if (this.timer != null) {
                if (this.timerTask != null) {
                    this.timerTask.cancel();
                    this.timerTask = null;
                }
                this.timer.shutdown();
                this.timer = null;
            }
            this.stopServer(null);
            if (this.zkManager != null) {
                this.zkManager.close();
            }
            if (this.scheduleStrategyManager != null) {
                try {
                    CuratorFramework zk = this.scheduleStrategyManager.getZooKeeper();
                    if (zk != null) {
                        zk.close();
                    }
                } catch (Exception e) {
                    logger.error("stopAll zk getZooKeeper异常！", e);
                }
            }
            this.uuid = null;
            logger.info("stopAll 停止服务成功！");
        } catch (Throwable e) {
            logger.error("stopAll 停止服务失败：" + e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 重启所有的服务
     *
     * @throws Exception
     */
    public void reStart() throws Exception {
        try {
            if (this.timer != null) {
                if (this.timerTask != null) {
                    this.timerTask.cancel();
                    this.timerTask = null;
                }
                this.timer.purge();
            }
            this.stopServer(null);
            if (this.zkManager != null) {
                this.zkManager.close();
            }
            this.uuid = null;
            this.init();
        } catch (Throwable e) {
            logger.error("重启服务失败：" + e.getMessage(), e);
        }
    }

    public boolean isZookeeperInitialSucess() throws Exception {
        return this.zkManager.checkZookeeperState();
    }

    public String[] getScheduleTaskDealList() {
        return applicationContext.getBeanNamesForType(IScheduleTaskDeal.class);
    }

    public IScheduleDataManager getScheduleDataManager() {
        if (this.scheduleDataManager == null) {
            throw new RuntimeException(this.errorMessage);
        }
        return scheduleDataManager;
    }

    public ScheduleStrategyDataManager4ZK getScheduleStrategyManager() {
        if (this.scheduleStrategyManager == null) {
            throw new RuntimeException(this.errorMessage);
        }
        return scheduleStrategyManager;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getIp() {
        return ip;
    }

    public String getHostName() {
        return hostName;
    }

    public void setStart(boolean isStart) {
        this.start = isStart;
    }

    public void setTimerInterval(int timerInterval) {
        this.timerInterval = timerInterval;
    }

    public ZKManager getZkManager() {
        return this.zkManager;
    }

    public Map<String, String> getZkConfig() {
        return zkConfig;
    }

    public void setZkConfig(Map<String, String> zkConfig) {
        this.zkConfig = zkConfig;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public Object getBean(String name) {
        return this.applicationContext.getBean(name);
    }
}

class ManagerFactoryTimerTask extends java.util.TimerTask {
    private static transient Logger logger = LoggerFactory.getLogger(ManagerFactoryTimerTask.class);
    private TBScheduleManagerFactory factory;
    private int count = 0;

    ManagerFactoryTimerTask(TBScheduleManagerFactory aFactory) {
        this.factory = aFactory;
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
            if (!this.factory.zkManager.checkZookeeperState()) {
                if (count > 5) {
                    logger.error("Zookeeper连接失败，关闭所有的任务后，重新连接Zookeeper服务器......");
                    this.factory.reStart();

                } else {
                    count = count + 1;
                }
            } else {
                count = 0;
                this.factory.refresh();
            }
        } catch (Throwable ex) {
            logger.error(ex.getMessage(), ex);
        } finally {
            factory.timerTaskHeartBeatTS = System.currentTimeMillis();
        }
    }
}

class InitialThread extends Thread {
    private static transient Logger log = LoggerFactory.getLogger(InitialThread.class);
    TBScheduleManagerFactory facotry;
    boolean isStop = false;

    InitialThread(TBScheduleManagerFactory aFactory) {
        this.facotry = aFactory;
    }

    public void stopThread() {
        this.isStop = true;
    }

    @Override
    public void run() {
        facotry.lock.lock();
        try {
            int count = 0;
            while (!facotry.zkManager.checkZookeeperState()) {
                count = count + 1;
                if (count % 50 == 0) {
                    facotry.errorMessage = "Zookeeper connecting ......" + facotry.zkManager.getConnectStr() + " spendTime:" + count * 20 + "(ms)";
                    log.error(facotry.errorMessage);
                }
                Thread.sleep(20);
                if (this.isStop) {
                    return;
                }
            }
            facotry.initialData();
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        } finally {
            facotry.lock.unlock();
        }
    }
}
