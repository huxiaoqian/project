# -*- coding: utf-8 -*-

import redis
from config import REDIS_HOST, REDIS_PORT
from domain_user import getDomainUsers
from domain import _domains_active

DOMAIN_USERS = "domain_users:%s" # domain

def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def _add_domain_users(r, domainid):
		domain_users = getDomainUsers(domainid)
		for uid in domain_users:
		    r.sadd(DOMAIN_USERS % domainid, uid)
    

if __name__ == '__main__':
		r = _default_redis()

		domains = _domains_active()
		for domain in domains:
		    _add_domain_users(r, domain['idx'])