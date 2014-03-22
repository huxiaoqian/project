# -*- coding: utf-8 -*-

import os
import sys
import leveldb
from SSDB import SSDB
from config import SSDB_HOST, SSDB_PORT, LEVELDBPATH

DOMAIN_TOPK_KEYWORDS_SSDB_HASH = 'domain_topk_keywords_hash:%s' # datestr as '20130901'
DOMAIN_AIRO_SSDB_HASH = 'domain_airo_hash:%s' # datestr as '20130901'


def profile_domain_keywords2ssdb():
	  for i in range(9, 21):
	  	  keywords_bucket = daily_profile_domain_topk_keywords_bucket[i]
	  	  for k, v in keywords_bucket.RangeIter():
	  	   	  print k, v
	  	   	  print now_datestr, ' saved ', ssdb.request('hset', [DOMAIN_TOPK_KEYWORDS_SSDB_HASH % now_datestr, str(i), v])


def profile_domain_airo2ssdb():
    for k, v in daily_profile_domain_airo_bucket.RangeIter():
        print k, v
        print now_datestr, ' saved ', ssdb.request('hset', [DOMAIN_AIRO_SSDB_HASH % now_datestr, k, v])


if __name__ == '__main__':
    now_datestr = sys.argv[1]

    daily_profile_domain_topk_keywords_bucket = {}
    for i in range(9, 21):
        daily_profile_domain_topk_keywords_bucket[i] = leveldb.LevelDB(os.path.join(LEVELDBPATH, './domain_keywords/linhao_profile_domain_topk_keywords_%s_%s' % (now_datestr, i)),
                                                                       block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    
    daily_profile_domain_airo_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_%s' % now_datestr),
                                                       block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    try:
        ssdb = SSDB(SSDB_HOST, SSDB_PORT)
    except Exception , e:
        print 'ssdb connect failed', e 
        sys.exit(0)

    profile_domain_keywords2ssdb()
    profile_domain_airo2ssdb()
