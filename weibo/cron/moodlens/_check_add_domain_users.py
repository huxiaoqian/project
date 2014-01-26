# -*- coding: utf-8 -*-

import os
import redis
import leveldb
from config import REDIS_HOST, REDIS_PORT, LEVELDBPATH, DOMAIN_LIST
from domain_user import getDomainUsers
from domain import _domains_active

DOMAIN_USERS = "domain_users:%s" # domain

def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def _add_domain_users(r, domainid):
    domain_users = getDomainUsers(domainid)
    for uid in domain_users:
        r.sadd(DOMAIN_USERS % domainid, uid)


def _add_domain_usersFromLeveldb(updatetime='20131220'):
    try:
        spieduser_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'spiedusers_4'),
                                           block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    except:
        print 'spieduser_bucket leveldb not available now'

    for k, v in spieduser_bucket.RangeIter():
        uid, updatetime = k.split('_')
        uid = int(uid)
        domain = str(v)
        domainid = int(DOMAIN_LIST.index(domain))
        r.sadd(DOMAIN_USERS % domainid, uid)


def _clear_domain_users(r, domainid):
    r.delete(DOMAIN_USERS % domainid)
    

if __name__ == '__main__':
    r = _default_redis()
    
    '''
    # add 8 domain users
    domains = _domains_active()
    for domain in domains:
                    #_clear_domain_users(r, domain['idx'])
        _add_domain_users(r, domain['idx'])
    '''

    # add domain users from leveldb
    _add_domain_usersFromLeveldb()
