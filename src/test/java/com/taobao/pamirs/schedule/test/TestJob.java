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
 * @title TestJob
 * @package com.taobao.pamirs.schedule.test
 * @description
 * @since 2019/4/10 11:47
 */
@Component("testJob")
public class TestJob implements IScheduleTaskDealSingle<String> {
    @Override
    public boolean execute(String task, String ownSign) throws Exception {
        System.out.println(task);
        return true;
    }

    @Override
    public List<String> selectTasks(String taskParameter, String ownSign, int taskItemNum, List<TaskItemDefine> taskItemList, int eachFetchDataNum, int pageNum) throws Exception {
        List<String> data = new ArrayList<String>(){
            private static final long serialVersionUID = -3952499302818550582L;
            {
                add("a");
                add("a");
                add("a");
                add("a");
                add("a");
                add("a");
                add("a");
                add("a");
            }
        };
        return data;
    }

    @Override
    public Comparator<String> getComparator() {
        return null;
    }
}
