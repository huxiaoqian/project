# -*- coding: utf-8 -*-

import os
import sys
import time
import leveldb
from config import LEVELDBPATH


def split_keywords_db():
    count = 0
    ts = te = time.time()
    for k, v in daily_profile_keywords_db.RangeIter():
        daily_profile_kic_db.Put('k' + '_' + now_datestr + '_' + k, v)

        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), 'profile_split_keywords_db', now_datestr
            ts = te
        count += 1


def split_interact_db():
    count = 0
    ts = te = time.time()
    for k, v in daily_profile_interact_db.RangeIter():
        daily_profile_kic_db.Put('i' + '_' + now_datestr + '_' + k, v)

        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), 'profile_split_interact_db', now_datestr
            ts = te
        count += 1


def split_counts_db():
    count = 0
    ts = te = time.time()
    for k, v in daily_profile_counts_db.RangeIter():
        daily_profile_kic_db.Put('c' + '_' + now_datestr + '_' + k, v)

        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), 'profile_split_counts_db', now_datestr
            ts = te
        count += 1


if __name__ == '__main__':
    now_datestr = sys.argv[1] # datestr as '20130901'

    daily_profile_keywords_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_keywords_%s' % now_datestr),
                                                block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    daily_profile_interact_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_interact_%s' % now_datestr),
                                                block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    daily_profile_counts_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_counts_%s' % now_datestr),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    daily_profile_kic_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_keywords_interact_counts_%s' % now_datestr),
                                           block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    
    split_keywords_db()
    split_interact_db()
    split_counts_db()
