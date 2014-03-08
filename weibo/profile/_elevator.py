# -*- coding: utf-8 -*-

import time
import os
import json
import pyelevator
from pyelevator import WriteBatch, Elevator
try:
    from weibo.global_config import LEVELDBPATH, ELEVATOR_HOST, ELEVATOR_PORT
except:
    LEVELDBPATH = '/home/mirage/leveldb'
    ELEVATOR_HOST = '192.168.2.31'
    ELEVATOR_PORT = 4141
    print 'not in web environment'


def _default_elevator(db_name='default'):
    db = Elevator(db_name, transport='tcp', endpoint='%s:%s' % (ELEVATOR_HOST, ELEVATOR_PORT))
    return db


def init_db(datestr):
    try:
        E.createdb(os.path.join(LEVELDBPATH, 'linhao_profile_person_%s' % datestr))
    except Exception, e:
        print e
    try:
        E.createdb(os.path.join(LEVELDBPATH, 'linhao_profile_person_%s_test' % datestr))
    except Exception, e:
        print e
    try:
        E.createdb(os.path.join(LEVELDBPATH, 'linhao_profile_domain_%s' % datestr))
    except Exception, e:
        print e
    try:
        E.createdb(os.path.join(LEVELDBPATH, 'linhao_profile_domain_rtkeywords_%s' % datestr))
    except Exception, e:
        print e
    try:
        E.createdb(os.path.join(LEVELDBPATH, 'linhao_profile_domain_basic_%s' % datestr))
    except Exception, e:
        print e
    E.disconnect()


def getPersonData(uid, datestr):
    active = important = reposts = original = emoticon = 0
    direct_interact = {}
    retweeted_interact = {}
    keywords_dict = {}

    try:
        level = _default_elevator(os.path.join(LEVELDBPATH, 'linhao_profile_person_%s' % datestr))
    except Exception, e:
        print e
        return active, important, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict

    try:
        results = level.Get(str(uid))
        active, important, reposts, original, emoticon, interact_dict, keywords_dict = results.split('_\/')
        active = int(active)
        important = int(important)
        reposts = int(reposts)
        original = int(original)
        emoticon = int(emoticon)
        interact_dict = json.loads(interact_dict)
        direct_interact = interact_dict['direct']
        retweeted_interact = interact_dict['retweeted']
        keywords_dict = json.loads(keywords_dict)
    except KeyError:
        pass

    try:
    	level.disconnect()
    except Exception, e:
    	print e

    return active, important, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict


def getDomainCountData(domain, datestr):
    # domain: -1~20
    try:
        level = _default_elevator(os.path.join(LEVELDBPATH, 'linhao_profile_domain_%s' % datestr))
    except Exception, e:
        print e
        active = important = reposts = original = 0
        return active, important, reposts, original

    try:
        results = level.Get(str(domain))
        active, important, reposts, original= results.split('_\/')
        active = int(active)
        important = int(important)
        reposts = int(reposts)
        original = int(original)
    except KeyError:
        active = important = reposts = original = 0

    try:
    	level.disconnect()
    except Exception, e:
    	print e

    return active, important, reposts, original


def getDomainKeywordsData(domain, datestr):
    # domain: -1~20
    try:
        level = _default_elevator(os.path.join(LEVELDBPATH, 'linhao_profile_domain_rtkeywords_%s' % datestr))
    except Exception, e:
        print e
        return {}

    try:
        results = level.Get(str(domain))
        keywords_dict = json.loads(results)
    except KeyError:
        keywords_dict = {}

    try:
    	level.disconnect()
    except Exception, e:
    	print e

    return keywords_dict


def getDomainBasic(domain, datestr):
    try:
        level = _default_elevator(os.path.join(LEVELDBPATH, 'linhao_profile_domain_basic_%s' % datestr))
    except Exception, e:
        print e
        return 0, 0, {}

    try:
        results = level.Get(str(domain))
        verified_count, unverified_count, province_dict = results.split('_\/')
        province_dict = json.loads(province_dict)
    except KeyError:
        verified_count = unverified_count = 0 
        province_dict = {}

    try:
    	level.disconnect()
    except Exception, e:
    	print e

    return verified_count, unverified_count, province_dict


if __name__ == '__main__':
    import sys
    datestr = sys.argv[1] # '20130903'
    E = _default_elevator()
    #E.connect(os.path.join(LEVELDBPATH, 'linhao_profile_domain_%s' % datestr))
    #E.repairdb()
    init_db(datestr)
