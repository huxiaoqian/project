# -*- coding: utf-8 -*-

import os
import time
import leveldb
from config import xapian_search_user, LEVELDBPATH

user_name_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_name'),
                                                block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
users = xapian_search_user.iter_all_docs(fields=['user', 'name'])

count = 0
ts = te = time.time()
for user in users:
    uid = user['user']
    name = user['name']
    user_name_bucket.Put(str(name.encode('utf-8')), str(uid))
    
    if count % 10000 == 0:
        te = time.time()
        print count, '%s sec' % (te - ts), ' user name to leveldb'
        ts = te
    count += 1
    
