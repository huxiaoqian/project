# -*- coding: utf-8 -*-

import os
import json
import leveldb
try:
    from weibo.global_config import LEVELDBPATH
except:
    LEVELDBPATH = '/home/mirage/leveldb'
    print 'not in web environment'


def getProfilePersonDbByDate(datestr):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_%s' % datestr), \
                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_db


def getProfileDomainDbByDate(datestr):
    # datestr '20140105'
    daily_domain_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_%s' % datestr), \
                                      block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_domain_db


def get_daily_domain_rtkeywords_db_by_date(datestr):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_rtkeywords_%s' % (datestr)),
                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_db


def get_daily_domain_basic_db_by_date(datestr):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_basic_%s' % datestr),
                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_db


def getPersonData(uid, datestr):
    level = getProfilePersonDbByDate(datestr)
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
        active = important = reposts = original = emoticon = 0
        direct_interact = {}
        retweeted_interact = {}
        keywords_dict = {}

    return active, important, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict


def getDomainCountData(domain, datestr):
    # domain: -1~20
    level = getProfileDomainDbByDate(datestr)
    try:
        results = level.Get(str(domain))
        active, important, reposts, original= results.split('_\/')
        active = int(active)
        important = int(important)
        reposts = int(reposts)
        original = int(original)
    except KeyError:
        active = important = reposts = original = 0

    return active, important, reposts, original


def getDomainKeywordsData(domain, datestr):
    # domain: -1~20
    level = get_daily_domain_rtkeywords_db_by_date(datestr)
    try:
        results = level.Get(str(domain))
        keywords_dict = json.loads(results)
    except KeyError:
        keywords_dict = {}

    return keywords_dict


def getDomainBasic(domain, datestr):
    level = get_daily_domain_basic_db_by_date(datestr)
    try:
        results = level.Get(str(domain))
        verified_count, unverified_count, province_dict = results.split('_\/')
        province_dict = json.loads(province_dict)
    except KeyError:
        verified_count = unverified_count = 0 
        province_dict = {}

    return verified_count, unverified_count, province_dict


def main():
    global_level = getProfileDomainDbByDate('20130901')
    for k, v in global_level.RangeIter():
        print k
        active, important, reposts, original = v.split('_\/')


if __name__ == '__main__':
    main()