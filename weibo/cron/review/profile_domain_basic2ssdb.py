# -*- coding: utf-8 -*-

import os
import sys
import leveldb
from SSDB import SSDB
from config import SSDB_HOST, SSDB_PORT, LEVELDBPATH


DOMAIN_BASIC_SSDB_HASH = 'domain_basic_hash:%s' # datestr as '20130901'


def profile_domain_basic():
    for k, v in profile_domain_basic_leveldb.RangeIter():
        print k, v
        print now_datestr, ' saved ', ssdb.request('hset', [DOMAIN_BASIC_SSDB_HASH % now_datestr, k, v])


if __name__ == '__main__':
    # init ssdb
    try:
        ssdb = SSDB(SSDB_HOST, SSDB_PORT)
    except Exception , e:
        print 'ssdb connect failed', e 
        sys.exit(0)

    # init leveldb
    now_datestr = '20130901'
    profile_domain_basic_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_basic_%s' % now_datestr),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    profile_domain_basic()
