# 分布式MySQL 指南

**作者: linhao.** 

## 第一节 概述

Cobar是阿里巴巴（B2B）部门开发的一种关系型数据的分布式处理系统，它可以在分布式的环境下看上去像传统数据库一样为您提供海量数据服务。

cobar中间件以proxy的形式位于前台应用和实际数据库之间，对前台的开放的接口是mysql通信协议，将前台SQL语句变更并按照数据分布规则转发到合适的后台数据分库，再合并返回结果，模拟单库下的数据库行为。

![image](http://img1.guokr.com/image/RWHG3pAD_a4IRrDCHxuGKkaPaX4uBnJdZX1FRcUe2s3JAQAAowEAAFBO.png)

## 第二节 应用场景

### 场景说明

1 系统对外提供的数据库名是cobar_db_weibo, 部署在节点192.168.2.31上，并且其中有一张表profile_person_basic。

2 profile_person_basic表分别有一部分数据被映射到3个不同物理节点（192.168.2.31、192.168.2.32、192.168.2.41）物理数据库cobar_weibo的profile_person_basic表上。如下图所示：

![image](https://raw.githubusercontent.com/linhaobuaa/project/master/snapshot/cobar.png?token=1652264__eyJzY29wZSI6IlJhd0Jsb2I6bGluaGFvYnVhYS9wcm9qZWN0L21hc3Rlci9zbmFwc2hvdC9jb2Jhci5wbmciLCJleHBpcmVzIjoxMzk4MDgyNjQyfQ%3D%3D--fedd105a74ba57de6041558f1848ed5dd959c34a)

### 路由规则配置
"""conf/rule.xml"""
```
<!DOCTYPE cobar:rule SYSTEM "rule.dtd">
<cobar:rule xmlns:cobar="http://cobar.alibaba.com/">

  <!-- 路由规则定义，定义什么表，什么字段，采用什么路由算法 -->
  <tableRule name="rule1">
    <rule>
      <columns>userId</columns>
      <algorithm><![CDATA[ func1(${userId}) ]]></algorithm>
    </rule>
  </tableRule>

  <!-- 路由函数定义 -->
  <function name="func1" class="com.alibaba.cobar.route.function.PartitionByLong">
    <property name="partitionCount">2,1</property>
    <property name="partitionLength">362,300</property>
  </function>

</cobar:rule>
```

### 数据节点配置
"""conf/schema.xml"""
```
<!DOCTYPE cobar:schema SYSTEM "schema.dtd">
<cobar:schema xmlns:cobar="http://cobar.alibaba.com/">

  <!-- schema定义 -->
  <schema name="cobar_db_weibo" dataNode="dnTest1">
    <table name="profile_person_basic" dataNode="dnTest1,dnTest2,dnTest3" rule="rule1" />
    <table name="profile_person_basic_test" dataNode="dnTest1,dnTest2,dnTest3" rule="rule1" />
    <table name="profile_person_weibo_count" dataNode="dnTest1,dnTest2,dnTest3" rule="rule1" />
  </schema>

  <!-- 数据节点定义，数据节点由数据源和其他一些参数组织而成。-->
  <dataNode name="dnTest1">
    <property name="dataSource">
      <dataSourceRef>dsTest[0]</dataSourceRef>
    </property>
  </dataNode>
  <dataNode name="dnTest2">
    <property name="dataSource">
      <dataSourceRef>dsTest[1]</dataSourceRef>
    </property>
  </dataNode>
  <dataNode name="dnTest3">
    <property name="dataSource">
      <dataSourceRef>dsTest[2]</dataSourceRef>
    </property>
  </dataNode>

  <!-- 数据源定义，数据源是一个具体的后端数据连接的表示。-->
  <dataSource name="dsTest" type="mysql">
    <property name="location">
      <location>192.168.2.31:3306/cobar_weibo</location>
      <location>192.168.2.32:3306/cobar_weibo</location>
      <location>192.168.2.41:3306/cobar_weibo</location>
    </property>
    <property name="user">root</property>
    <property name="password"></property>
    <property name="sqlMode">STRICT_TRANS_TABLES</property>
  </dataSource>

</cobar:schema>
```
[读](https://github.com/linhaobuaa/project/blob/master/weibo/profile/_mysql.py)

### 写数据库

[写]https://github.com/linhaobuaa/project/blob/master/weibo/cron/profile/cron_job.py

```
def iter_userbasic2mysql(cobar_conn, sharding=False):
    cursor = cobar_conn.cursor()

    # drop non-primary indexes on table
    try:
        cursor.execute("SHOW INDEX FROM profile_person_basic")
        results = cursor.fetchall()
        for r in results:
            index_name = r[2]
            if index_name != 'PRIMARY':
                cursor.execute("DROP INDEX %s ON profile_person_basic" % index_name)
    except Exception, e:
        print e
    print 'drop indexes completely'

    # delete data from table
    try:
        cursor.execute("DELETE FROM profile_person_basic")
        cobar_conn.commit()
    except Exception, e:
        cobar_conn.rollback()
        print e
    print 'clean table data completely'

    # insert new data
    count = 0
    ts = te = time.time()
    for k, v in daily_profile_person_basic_db.RangeIter():
        if count % 2000 == 0:
            if sharding:
                # Commit your changes in the database
                cobar_conn.commit()

        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), 'leveldb2mysql person basic'
            ts = te

        userId = int(k)
        
        try:
    	    province, city, verified, name, friendsCount, gender, profileImageUrl, verifiedType, followersCount, location, statusesCount, description, created_at, domain = v.split('_\/')
        except Exception, e:
            print e
            count += 1
            continue
        description = _utf_encode(description)
        domain = _utf_encode(domain)
        verified = 1 if verified == 'True' else 0
        date = batch_date_1
        
        if sharding:
            sql = """insert into profile_person_basic(userId, province, city, verified, name, gender, \
                     profileImageUrl, verifiedType, friendsCount, followersCount, statuseCount, location, \
                     created_at, description, domain) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', 
                     '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (userId, province, city, verified, name, gender, \
                     profileImageUrl, verifiedType, friendsCount, followersCount, statusesCount, location, \
                     created_at, description, domain)

            try:
                cursor.execute(sql)
            except Exception, e:
                #print e
                description = ''
                sql = """insert into profile_person_basic(userId, province, city, verified, name, gender, \
                     profileImageUrl, verifiedType, friendsCount, followersCount, statuseCount, location, \
                     created_at, description, domain) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', 
                     '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (userId, province, city, verified, name, gender, \
                     profileImageUrl, verifiedType, friendsCount, followersCount, statusesCount, location, \
                     created_at, description, domain)
            
                cursor.execute(sql)

        count += 1

    # create index
    print 'create cbw_ppb_userid index on table profile_person_basic'
    try:
        cursor.execute("CREATE INDEX cbw_ppb_userid on profile_person_basic(userId)")
    except Exception, e:
        print e

    print 'create cbw_ppb_fol_fri_sta_loc index on table profile_person_basic'
    try:
        cursor.execute("CREATE INDEX cbw_ppb_fol_fri_sta_loc on profile_person_basic(followersCount, friendsCount, statuseCount, location)")
    except Exception, e:
        print e

    print 'create cbw_ppb_created index on table profile_person_basic'
    try:
        cursor.execute("CREATE INDEX cbw_ppb_created on profile_person_basic(created_at)")
    except Exception, e:
        print e

    print 'create cbw_ppb_fol_domain index on table profile_person_basic'
    try:
        cursor.execute("CREATE INDEX cbw_ppb_fol_domain on profile_person_basic(followersCount, domain)")
    except Exception, e:
        print e

    if cobar_conn:
        cobar_conn.close()
```

## 附录 安装指南

可参考cobar官方网站代码

[github代码库](https://github.com/alibaba/cobar)

[cobar 官方网站](http://code.alibabatech.com/wiki/display/cobar/Home)
