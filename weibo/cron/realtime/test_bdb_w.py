# -*- coding: utf-8 -*-

import os
import sys
import time
import bsddb3
import leveldb
from bsddb3 import db
from config import LEVELDBPATH

BDB_DATA_PATH = '/media/data/berkeley/data'
BDB_LOG_PATH = '/media/data/berkeley/log'
BDB_TMP_PATH = '/media/data/berkeley/tmp'



def write_from_seperate_text():
    count = 0
    ts = te = time.time()
    for k, v in profile_interact_leveldb.RangeIter():
        profile_interact_hash_db.put(str(now_datestr) + '_' + k, str(v))
    
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), 'bdb write ', now_datestr
            ts = te
        count += 1


def write_from_whole_text():
    count = 0
    ts = te = time.time()
    for k, v in profile_person_leveldb.RangeIter():
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), 'bdb write from whole text', now_datestr
            ts = te
        count += 1

    	uid = k
        active, important, reposts, original, emoticon, interact_dict, keywords_dict = v.split('_\/')
        profile_interact_hash_db.put(str(now_datestr) + '_' + k, str(interact_dict))
        profile_keywords_hash_db.put(str(now_datestr) + '_' + k, str(keywords_dict))
        profile_counts_hash_db.put(str(now_datestr) + '_' + k, '_\/'.join([active, important, reposts, original, emoticon]))


if __name__ == '__main__':
    db_env = db.DBEnv()
    db_env.set_tmp_dir(BDB_TMP_PATH)
    db_env.set_lg_dir(BDB_LOG_PATH)
    db_env.set_cachesize(0, 8 * (2 << 25), 1)
    db_env.open(BDB_DATA_PATH, db.DB_INIT_CDB | db.DB_INIT_MPOOL | db.DB_CREATE)
    
    # init bdb
    profile_interact_hash_db = db.DB(db_env)
    profile_interact_hash_db.open('profile_person_interact_hash', None, db.DB_HASH, db.DB_CREATE)
    
    profile_keywords_hash_db = db.DB(db_env)
    profile_keywords_hash_db.open('profile_person_keywords_hash', None, db.DB_HASH, db.DB_CREATE)
    
    profile_counts_hash_db = db.DB(db_env)
    profile_counts_hash_db.open('profile_person_counts_hash', None, db.DB_HASH, db.DB_CREATE)

    # init leveldb
    now_datestr = sys.argv[1]
    profile_interact_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_interact_%s' % now_datestr),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    profile_person_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_%s' % now_datestr),
                                             block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))    
    
    write_from_whole_text()

    # close
    profile_interact_hash_db.close()
    profile_keywords_hash_db.close()
    profile_counts_hash_db.close()
