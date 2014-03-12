# -*- coding: utf-8 -*-

import os
import sys
import time
import leveldb
from config import xapian_search_user, COBAR_HOST, COBAR_PORT, COBAR_USER, LEVELDBPATH

xapian_user_fields = ['_id', 'province', 'city', 'verified', 'name', 'friends_count', \
                     'gender', 'profile_image_url', 'verified_type','followers_count', \
                     'location', 'statuses_count', 'description', 'created_at']

def iter_userbasic2leveldb():
    users = xapian_search_user.iter_all_docs(fields=xapian_user_fields)

    count = 0
    batch = leveldb.WriteBatch()
    ts = te = time.time()
    for user in users:
        if count % 10000 == 0:
            te = time.time()
            daily_profile_person_basic_db.Write(batch, sync=True)
            batch = leveldb.WriteBatch()
            print count, '%s sec' % (te - ts), 'xapian2leveldb person basic'
            ts = te
        # extraction and transfer
        try:
            userId = int(user['_id'])
        except:
            count += 1
            continue
        province = user['province']
        city = user['city']
        verified = user['verified']
        name = _utf_encode(user['name'])
        friendsCount = user['friends_count']
        gender = user['gender']
        profileImageUrl = user['profile_image_url']
        verifiedType = user['verified_type']
        followersCount = user['followers_count']
        location = _utf_encode(user['location'])
        statusesCount = user['statuses_count']
        description = _utf_encode(user['description'])
        domain = userLeveldb2DomainZh(userId)
        
        try:
            created_at = int(user['created_at'])
        except:
            count += 1
            continue

        date = batch_date_1
        #Load
        key = str(userId)
        value = '_\/'.join([str(province), str(city), str(verified), \
                            str(name), str(friendsCount), str(gender), \
                            str(profileImageUrl), str(verifiedType), \
                            str(followersCount), str(location), \
                            str(statusesCount), str(description), \
                            str(created_at), str(domain)])
        batch.Put(key, value)

        count += 1


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


def get_now_datestr():
    return datetime.datetime.now().strftime("%Y%m%d")


if __name__ == '__main__':
	now_datestr = get_now_datestr()

	# update person basics once a week
    sharding = False
    if sharding:
        # mysqldb连接数据库　
        try:
            cobar_conn = MySQLdb.connect(host=COBAR_HOST, user=COBAR_USER, db='cobar_db_weibo', port=COBAR_PORT, charset='utf8')
            print 'connection success'
        except Exception, e:
            print e
            sys.exit()

	iter_userbasic2leveldb()
    iter_userbasic2mysql(cobar_conn, sharding)
	daily_profile_person_basic_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_basic_%s' % now_datestr),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
