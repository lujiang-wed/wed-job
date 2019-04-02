# wed-job

## tbschedule-wed正式更名为wed-job并发布第一个正式版 ##

[![Gitter](https://badges.gitter.im/wednesday-lj/wed-job.svg)]
(https://gitter.im/wednesday-lj/wed-job?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)  
[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)

基于TBSchedule官方3.2.18的重置版。TBSchedule具有框架轻、0入侵、效率高（高过elastic-job和xxl-job）等诸多特点，无奈官方于3.3.3.2版本后停止了升级与BUG修复。如此好的分布式任务调度框架弃之可惜，所以就让作者来捡个便宜吧（官方版所有地址均已下线，作者找不到原始开源协议故使用Apache 2.0以示尊重）。

重置版主要在官方原版的基础上优化了以下几点： 
>
1. 任务项状态管理全部改为顺序操作，牺牲一定的并发效能提升稳定性 
2. 解决在以往的实践使用中（大量job共用一个调度中心切job执行频繁场景）官方版偶尔出现任务不能正确停止与注销，造成任务项死循环执行，CPU满负载且产生大量脏日志的问题；
3. 优化在ZooKeeper集群不稳定时，策略与任务的注册与反注册



### 用户手册
基础用法与官方版一致，新手可以参考：https://my.oschina.net/wednesday/

### 构建
    git clone https://github.com/hungki/wed-job
	mvn clean package

**RELEASE NOTE**

Feb 28 2019-1.0.1_GA：

1. 修复Sleep时间短(ex:500ms)下，使用Timer类BUG导致Task停摆的问题
2. 使用CuratorFramework代替官方ZooKeeper API已获得更好的链接稳定性已经重连机制
3. 更新Java支持到[1.7,1.8)
4. 整理项目结构

Nov 5 2018-1.0.0：

1. job实现器追加pageNum参数（根据eachFetchDataNum和FetchCount计算得出）
2. 修订文档
3. 变更软件名及版本号

**TODO**

1. add:新版Console
2. fix:彻底修复高频词运行下任务假死的问题
