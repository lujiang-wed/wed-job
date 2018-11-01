# tbschedule-wed
基于TBSchedule官方3.3.3.2的改良版。主要在官方原版的基础上优化了以下几点： 1、任务项状态管理全部改为顺序操作，牺牲一定的并发效能提升稳定性； 2、解决在以往的实践使用中（大量job共用一个调度中心切job执行频繁场景）官方版偶尔出现任务不能正确停止与注销，造成任务项死循环执行，CPU满负载且产生大量脏日志的问题； 3、优化在ZooKeeper集群不稳定时，策略与任务的注册与反注册。

[![Gitter](https://badges.gitter.im/wednesday-lj/wed-job.svg)](https://gitter.im/wednesday-lj/wed-job?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)   [![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

一、基础用法与官方版一致，新手可以参考：http://geek.csdn.net/news/detail/65738

# DIFF点：

1、job实现器追加pageNum参数（根据eachFetchDataNum和FetchCount计算得出）

### example：
IScheduleTaskDeal.List<T> selectTasks(String taskParameter, String ownSign, int taskItemNum, List<TaskItemDefine> taskItemDefines, int eachFetchDataNum, int pageNum) throws Exception;
  
作用：
  a）原版tbs的取数是一直前进的，这可能导致某些不允许出现fail的应用场景下，处理fail后task会积压直到第二轮调度开始fatchCount内部重置以后才能再次被处理。加入pageNum参数后，用户可自行实现这样一道补偿机制：自行修改pageNum强制任务从头开始执行而不用打断job执行。
  
  b）对于要求需要知道数据startIndex以及endIndex场景时，pageNum的加入可以免去totalCount查询操作。
