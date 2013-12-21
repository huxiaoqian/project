# -*- coding: utf-8 -*-

import leveldb
import datetime
import time
import os
import sys

LEVELDBPATH = '/home/mirage/leveldb'
user_daily_field_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_field'),
                                          block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology']

def initial_load_user_field_from_mysql(update_date='20130430'):
    sys.path.append('../../profile')
    from config import db
    from model import UserField

    count = 0
    ts = te = time.time()
    daily_fields_kv = {}
    results = db.session.query(UserField).all()
    for r in results:
        uid = r.uid
        try:
            fields = ','.join([r.fieldFirst, r.fieldSecond])
            if r.fieldFirst not in fields_value or r.fieldSecond not in fields_value:
                count += 1
                continue
        except TypeError,e:
            fields = r.fieldFirst
            if r.fieldFirst not in fields_value:
                count += 1
                continue
        daily_fields_kv[str(uid) + '_' + str(update_date)] = fields
        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te

    batch = leveldb.WriteBatch()
    for k, v in daily_fields_kv.iteritems():
        batch.Put(k, v)
    user_daily_field_bucket.Write(batch, sync=True)


def test_fields():
    iter_all = user_daily_field_bucket.RangeIter()
    for k, v in iter_all:
        print k, v


def user_field_levedb2mysql():
    sys.path.append('../moodlens')
    from config import db
    from model import DomainUser

    iter_all = user_daily_field_bucket.RangeIter()
    ts = te = time.time()
    count = 0
    for k, v in iter_all:
        uid, update_str = k.split('_')
        domains = v
        item = DomainUser(int(uid), domains, update_str)
        db.session.add(item)
        
        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            db.session.commit()
            ts = te


if __name__ == '__main__':
    #test_fields()

    user_field_levedb2mysql()

    #initial_load_user_field_from_mysql()


