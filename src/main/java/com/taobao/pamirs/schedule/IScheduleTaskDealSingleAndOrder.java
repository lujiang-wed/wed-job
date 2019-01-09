package com.taobao.pamirs.schedule;

import java.util.List;

/**
 * 单个任务处理的接口
 * @author xuannan
 *
 * @param <T>任务类型
 */
public interface IScheduleTaskDealSingleAndOrder<T> extends IScheduleTaskDealSingle<T> {


    public List<T> selectTasksByOrder(String taskParameter, String ownSign, int taskItemNum, List<TaskItemDefine> taskItemList, int eachFetchDataNum, DataCursor dataCursor) throws Exception;
  
}
