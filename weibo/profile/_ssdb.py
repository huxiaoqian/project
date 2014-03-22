# -*- coding: utf-8 -*-

import sys
import json
from SSDB import SSDB
from weibo.global_config import SSDB_PORT, SSDB_HOST


try:
	  ssdb = SSDB(SSDB_HOST, SSDB_PORT)
except Exception , e:
	  print 'ssdb ', e 
	  sys.exit(0)

DOMAIN_TOPK_KEYWORDS_SSDB_HASH = 'domain_topk_keywords_hash:%s' # datestr as '20130901'
DOMAIN_AIRO_SSDB_HASH = 'domain_airo_hash:%s' # datestr as '20130901'


def getPersonData(uid, datestr):
    active = important = reposts = original = emoticon = 0
    direct_interact = {}
    retweeted_interact = {}
    keywords_dict = {}

    c_ssdb_resp = ssdb.request('get', ['c_' + str(datestr) + '_' + str(uid)])
    i_ssdb_resp = ssdb.request('get', ['i_' + str(datestr) + '_' + str(uid)])
    k_ssdb_resp = ssdb.request('get', ['k_' + str(datestr) + '_' + str(uid)])

    if c_ssdb_resp.code == 'ok' and c_ssdb_resp.data:
    	active, important, reposts, original, emoticon = c_ssdb_resp.data.split('_\/')
        active = int(active)
        important = int(important)
        reposts = int(reposts)
        original = int(original)
        emoticon = int(emoticon)

    if i_ssdb_resp.code == 'ok' and i_ssdb_resp.data:
        interact_dict = json.loads(i_ssdb_resp.data)
        direct_interact = interact_dict['direct']
        retweeted_interact = interact_dict['retweeted']

    if k_ssdb_resp.code == 'ok' and k_ssdb_resp.data:
        keywords_dict = json.loads(k_ssdb_resp.data)

    return active, important, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict


def getDomainKeywordsData(domain, datestr):
    keywords_dict = {}
    ssdb_resp = ssdb.request('hget', [DOMAIN_TOPK_KEYWORDS_SSDB_HASH % datestr, str(domain)])
    if ssdb_resp.code == 'ok' and ssdb_resp.data:
        keywords_dict = json.loads(ssdb_resp.data)

    return keywords_dict


def getDomainBasic(domain, datestr):
    pass


def getDomainCountData(domain, datestr):
    active = important = reposts = original = 0
    ssdb_resp = ssdb.request('hget', [DOMAIN_AIRO_SSDB_HASH % datestr, str(domain)])
    if ssdb_resp.code == 'ok' and ssdb_resp.data:
        active, important, reposts, original = ssdb_resp.data.split('_\/')
        active = int(active)
        important = int(important)
        reposts = int(reposts)
        original = int(original)

    return active, important, reposts, original
    