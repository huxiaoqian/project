# 分布式MySQL 指南

**作者: linhao.** 

## 第一节 安装指南

可参考cobar官方网站代码

[github代码库](https://github.com/alibaba/cobar)

## 第二节 概述

### 读数据库

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
