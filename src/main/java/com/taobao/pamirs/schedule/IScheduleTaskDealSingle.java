package com.taobao.pamirs.schedule;

/**
 * 单个任务处理的接口
 *
 * @param <T>任务类型
 * @author xuannan
 */
public interface IScheduleTaskDealSingle<T> extends IScheduleTaskDeal<T> {
    /**
     * 执行单个任务
     *
     * @param task    Object
     * @param ownSign 当前环境名称
     * @throws Exception
     */
    public boolean execute(T task, String ownSign) throws Exception;

}
