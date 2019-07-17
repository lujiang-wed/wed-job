package com.taobao.pamirs.schedule.test;

import com.taobao.pamirs.schedule.IScheduleTaskDealSingle;
import com.taobao.pamirs.schedule.TaskItemDefine;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Wednesday
 * @version V1.0.0
 * @title SingleExcuteJobExample
 * @package com.taobao.pamirs.schedule.test
 * @description
 * @since 2019/7/17 10:27
 */
@Component
public class SingleExcuteJobExample implements IScheduleTaskDealSingle<String> {

    @Override
    public boolean execute(String task, String ownSign) throws Exception {
        System.out.println(task);
        return true;
    }

    @Override
    public List<String> selectTasks(String taskParameter, String ownSign, int taskItemNum, List<TaskItemDefine> taskItemList, int eachFetchDataNum, int pageNum) throws Exception {
        List<String> mockTasks = new ArrayList<>();
        mockTasks.add("asd");
        mockTasks.add("asdd");
        mockTasks.add("asddd");
        mockTasks.add("asdddd");
        mockTasks.add("asddddd");
        mockTasks.add("asdddddd");
        mockTasks.add("asddddddd");
        mockTasks.add("asdddddddd");
        return mockTasks;
    }

    @Override
    public Comparator<String> getComparator() {
        return null;
    }
}
