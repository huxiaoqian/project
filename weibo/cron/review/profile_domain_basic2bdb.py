# -*- coding: utf-8 -*-

import os
import sys
import leveldb
from bsddb3 import db
from config import LEVELDBPATH

BDB_DATA_PATH = '/media/data/berkeley/data'
BDB_LOG_PATH = '/media/data/berkeley/log'
BDB_TMP_PATH = '/media/data/berkeley/tmp'


def profile_domain_basic():
	  for k, v in profile_domain_basic_leveldb.RangeIter():
	      profile_domain_basic_hash_db.put(k, v)


if __name__ == '__main__':
    db_env = db.DBEnv()
    db_env.set_tmp_dir(BDB_TMP_PATH)
    db_env.set_lg_dir(BDB_LOG_PATH)
    db_env.set_cachesize(0, 8 * (2 << 25), 1)
    db_env.open(BDB_DATA_PATH, db.DB_INIT_CDB | db.DB_INIT_MPOOL | db.DB_CREATE)
    
    # init bdb
    profile_domain_basic_hash_db = db.DB(db_env)
    profile_domain_basic_hash_db.open('profile_domain_basic_hash', None, db.DB_HASH, db.DB_CREATE)
    
    # init leveldb
    now_datestr = '20130901'
    profile_domain_basic_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_basic_%s' % now_datestr),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    profile_domain_basic()

    profile_domain_basic_hash_db.close()
    db_env.close()
