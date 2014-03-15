# -*- coding: utf-8 -*-

import redis
from config import REDIS_HOST, REDIS_PORT


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def clear_redis_db_1_15():
	  for db_no in range(1, 16):
	  	  _default_redis(db=db_no).flushdb()


if __name__ == '__main__':
	  clear_redis_db_1_15()
	  