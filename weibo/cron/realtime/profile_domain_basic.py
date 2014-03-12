# -*- coding: utf-8 -*-

import os
import leveldb
from config import xapian_search_user, LEVELDBPATH


def batch_handle_domain_basic():
    count = 0
    ts = te = time.time()
    users = xapian_search_user.iter_all_docs(fields=['_id', 'verified', 'location']) 
    for user in users:
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), ' %s daily domain basic' % batch_date_1
            ts = te

        domainid = userLeveldb2Domain(user['_id'])
        verified = user['verified']
        province_str = user['location'].split(' ')[0]
        
        try:
            verified_count, unverified_count, province_dict = daily_profile_domain_basic_db.Get(str(domainid)).split('_\/')
            verified_count = int(verified_count)
            unverified_count = int(unverified_count)
            province_dict = json.loads(province_dict)
        except KeyError:
            verified_count = unverified_count = 0
            province_dict = {}

        if verified:
            verified_count += 1
        else:
            unverified_count += 1

        try:
            province_dict[province_str] += 1
        except KeyError:
            province_dict[province_str] = 1

        key = str(domainid)
        value = '_\/'.join([str(verified_count), str(unverified_count), json.dumps(province_dict)])
        daily_profile_domain_basic_db.Put(key, value)

        count += 1


def get_now_datestr():
    return datetime.datetime.now().strftime("%Y%m%d")


if __name__ == '__main__':
    now_datestr = get_now_datestr()

    daily_profile_domain_basic_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_basic_%s' % now_datestr),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    batch_handle_domain_basic()
