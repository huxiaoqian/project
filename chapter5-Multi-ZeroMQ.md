# Multi-ZeroMQ

## 流数据框架拓扑图

![image](https://raw.githubusercontent.com/linhaobuaa/project/master/snapshot/MultiZeroMQ.png?token=1652264__eyJzY29wZSI6IlJhd0Jsb2I6bGluaGFvYnVhYS9wcm9qZWN0L21hc3Rlci9zbmFwc2hvdC9NdWx0aVplcm9NUS5wbmciLCJleHBpcmVzIjoxMzk4OTIzNDY2fQ%3D%3D--d4d2364d43e028bcb40fb9919f415697ddebf3f0)

## 测试性能

1、8个索引构建进程速度为8000条/秒，单进程速度1200条/秒；

2、3个情绪增量计算进程速度为3000条/秒；

3、3个画像增量计算进程速度为3000条/秒；

4、各套流之间主要共享带宽，每套流带宽占用在30Mb/s.

## 部署

1、原始csv存储机器：
   ```
   ubuntu9@192.168.2.33
   ```

   原始csv数据存储位置：
   ```
   /home/ubuntu9/dev/original_data/csv/20130928_cut/
   ```

   控制分发的redis服务器（管理分发队列）
   ```
   VENT_REDIS_HOST = '192.168.2.30'
   ```

   
   分发队列包括：
   ```
   GLOBAL_CSV_QUEUE_INDEX = 'global_vent_queue:index'
   GLOBAL_CSV_QUEUE_SENTIMENT = 'global_vent_queue:sentiment'
   GLOBAL_CSV_QUEUE_PROFILE = 'global_vent_queue:profile'
   ```


   启动csv数据源发送数据程序：
   ```
   su ubuntu9
   cd /home/ubuntu9/linhao/xapian_weibo/zmq_topic_workspace/
   python xapian_zmq_vent_json.py
   ```
 

2、数据缓冲及分发机器：
   ```
   ubuntu8@192.168.2.34
   ```

   CSV数据缓存位置：
   ```
   CSV_FLOW_PATH = '/home/ubuntu8/dev/original_data/csv/flow/'
   ```

   接收数据流写csv文件
   ```
   su ubuntu8
   cd /home/ubuntu8/linhao/xapian_weibo/zmq_topic_workspace/
   python xapian_zmq_write_csv.py
   ```


   3套流的配置：

   数据流一（索引）：
   ```
   XAPIAN_ZMQ_VENT_HOST = '192.168.2.34' # vent and control host
   XAPIAN_ZMQ_VENT_PORT = 5580 # vent port
   XAPIAN_ZMQ_CTRL_VENT_PORT = 5581 # control port
   VENT_REDIS_HOST = '192.168.2.30'
   GLOBAL_CSV_FILES = 'global_vent_queue:index'
   CSV_FILEPATH = '/home/ubuntu8/dev/original_data/csv/flow/'
   ```
   开启分发器：
   ```
   su ubuntu8
   cd /home/ubuntu8/linhao/xapian_weibo/zmq_index/
   python xapian_zmq_vent.py
   ```
   开启计算器：
   ```
   ssh ubuntu12@192.168.2.31
   su ubuntu12
   cd /home/ubuntu8/linhao/xapian_weibo/zmq_index/
   python xapian_zmq_work.py
   ```

   数据流二（情感增量计算）：
   ```
   XAPIAN_ZMQ_VENT_HOST = '192.168.2.34' # vent and control host
   XAPIAN_ZMQ_VENT_PORT = 5582 # vent port
   XAPIAN_ZMQ_CTRL_VENT_PORT = 5583 # control port
   SENTIMENT_REDIS_HOST = '192.168.2.31'
   VENT_REDIS_HOST = '192.168.2.30'
   GLOBAL_CSV_FILES = 'global_vent_queue:sentiment'
   CSV_FILEPATH = '/home/ubuntu8/dev/original_data/csv/flow/'
   ```
   开启分发器：
   ```
   su ubuntu8
   cd /home/ubuntu8/linhao/xapian_weibo/zmq_sentiment/
   python xapian_zmq_vent.py
   ```
   开启计算器：
   ```
   ssh ubuntu11@192.168.2.30
   su ubuntu11
   cd /home/ubuntu11/linhao/xapian_weibo/zmq_sentiment/
   python xapian_zmq_work.py
   ```

   数据流三（画像增量计算）：
   ```
   XAPIAN_ZMQ_VENT_HOST = '192.168.2.34' # vent and control host
   XAPIAN_ZMQ_VENT_PORT = 5584 # vent port
   XAPIAN_ZMQ_CTRL_VENT_PORT = 5585 # control port
   PROFILE_REDIS_HOST = '192.168.2.30'
   VENT_REDIS_HOST = '192.168.2.30'
   GLOBAL_CSV_FILES = 'global_vent_queue:profile'
   CSV_FILEPATH = '/home/ubuntu8/dev/original_data/csv/flow/'
   ```
   开启分发器：
   ```
   su ubuntu8
   cd /home/ubuntu8/linhao/xapian_weibo/zmq_profile/
   python xapian_zmq_vent.py
   ```
   开启计算器：
   ```
   ssh ubuntu10@192.168.2.30
   su ubuntu10
   cd /home/ubuntu10/linhao/xapian_weibo/zmq_profile/
   python xapian_zmq_work.py
   ```
