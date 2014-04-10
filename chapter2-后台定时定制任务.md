# 后台定时定制任务编写

**作者: linhao.** 

## 第一节 安装指南

本部分需要配合使用Xapian、ZeroMQ、Redis、Mysql等。

### Redis 安装
参考[redis官网](http://redis.io/)
```easy_install redis
```

### Xapian与ZeroMQ安装
参考[xapian_weibo](http://github.com/MOON-CLJ/xapian_weibo/)


## 第二节 概述
后台定时定制任务主要包括如下两类：
* 后台话题定时计算任务；
* 增量计算任务；
* 24点计算任务

后台话题定时计算任务又可细分为：
* 话题情绪计算任务；
* 话题敏感博主识别任务；
* 话题传播计算；
* 单条微博传播计算

增量计算任务可细分为：
* 情绪增量计算；
* 用户关键词增量计算

24点计算任务可以细分为：
* 关键博主排序整点计算；
* 画像整点计算

## 第三节 增量计算任务

