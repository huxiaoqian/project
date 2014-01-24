# -*- coding: utf-8 -*-

import os
import time
import leveldb
import datetime
from config import xapian_search_user, LEVELDBPATH

field_daily_active_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_user_followers'),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))


def follower_leveldb():
    get_results = xapian_search_user.iter_all_docs(fields=['_id', 'followers_count'])

    count = 0
    ts = te = time.time()
    batch = leveldb.WriteBatch()
    for result in get_results:
        batch.Put(str(result['_id']), str(result['followers_count']))
        
        if count % 10000 == 0:
            te = time.time()
            field_daily_active_count_bucket.Write(batch, sync=True)
            batch = leveldb.WriteBatch()
            print count, '%s sec' % (te - ts), 'identify followers_count to leveldb '
            ts = te

        count = count + 1

    return 'Done'

if __name__ == "__main__":

    answer = follower_leveldb()
    print answer
