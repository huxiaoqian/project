# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch
from global_config import xapian_search_user
import leveldb
import datetime
import time
import os

LEVELDBPATH = '/home/mirage/leveldb'
field_daily_active_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_user_followers'),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))


def follower_leveldb():

    get_results = xapian_search_user.iter_all_docs(fields=['_id', 'followers_count'])

    count = 0
    for result in get_results:
        count = count + 1
        if count%10000 == 0:
            print count
        field_daily_active_count_bucket.Put(str(result['_id']),str(result['followers_count']))

    return 'Done'

if __name__ == "__main__":

    #followers_rank()
    answer = follower_leveldb()
    print answer
