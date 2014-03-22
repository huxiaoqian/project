# -*- coding: utf-8 -*-


import json
import MySQLdb
from config import xapian_search_user, emotions_kv, COBAR_HOST, \
                   COBAR_USER, COBAR_PORT
from time_utils import datetime2ts, ts2HourlyTime
from xapian_weibo.utils import top_keywords, gen_mset_iter
from config import db
from model import SentimentDomainCount, SentimentDomainKeywords, \
                  SentimentDomainTopWeibos, Domain
from sqlalchemy.exc import IntegrityError
from dynamic_xapian_weibo import getXapianWeiboByDate
import multiprocessing
from domain_user import _domain_user
from config import DOMAIN_LIST
from domain import _is_domain_active, _domain_en_name, _domains_active


Minute = 60
Fifteenminutes = 15 * 60
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24

TOP_KEYWORDS_LIMIT = 50
TOP_WEIBOS_LIMIT = 50
TOP_DOMAIN_LIMIT = 5000

RESP_ITER_KEYS = ['_id', 'user', 'retweeted_uid', 'retweeted_mid', 'text', 'timestamp', 'reposts_count', 'bmiddle_pic', 'geo', 'comments_count', 'sentiment', 'terms']

FIELDS_VALUE = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology', 'oversea']
FIELDS_ZH_NAME = [u'文化', u'教育', u'娱乐', u'时尚', u'财经', u'媒体', u'体育', u'科技', u'海外']
FIELDS2ID = {}
FIELDS2ZHNAME = {}

for key in FIELDS_VALUE:
    idx = FIELDS_VALUE.index(key)
    FIELDS2ID[key] = idx
    FIELDS2ZHNAME[key] = FIELDS_ZH_NAME[idx]


def getDomainUsers(domain, top=TOP_DOMAIN_LIMIT):
    active = _is_domain_active(domain)
    domain_en_name = _domain_en_name(domain)
    domain_uids = []

    if not active or not domain_en_name:
        return []

    if domain_en_name != 'oversea':
        domain_uids = _domain_user(domain_en_name, top)
    else:
        count, get_results = xapian_search_user.search(query={'location': FIELDS2ZHNAME['oversea']}, sort_by=['followers_count'], fields=['_id'], max_offset=top)
        for user in get_results():
            domain_uids.append(user['_id'])

    return domain_uids


def DomainUsersFromMysql(domain, limit=10000):
    # domain: university
    print domain, 'search domain users from mysql'
    try:
        cobar_conn = MySQLdb.connect(host=COBAR_HOST, user=COBAR_USER, db='cobar_db_weibo', port=COBAR_PORT, charset='utf8')
        cursor = cobar_conn.cursor()
        cursor.execute("select userId from profile_person_basic where domain = '%s' limit %d" % (domain, limit))
        users = cursor.fetchall()
        domain_uids = [int(user[0]) for user in users]
    except Exception, e:
        print e
        domain_uids = []

    return domain_uids

def save_count_results(domain, dic, during):
    for k, v in dic.iteritems():
        sentiment = k
        ts, count = v

        item = SentimentDomainCount(domain, during, ts, sentiment, count)
        item_exist = SentimentDomainCount.query.filter_by(domain=domain, range=during, ts=ts, sentiment=sentiment).first()

        if item_exist:
            db.session.delete(item_exist)
            db.session.add(item)
        else:
            db.session.add(item)

    db.session.commit()


def save_kcount_results(domain, dic, during, limit):
    for k, v in dic.iteritems():
        sentiment = k
        ts, kcount = v

        item = SentimentDomainKeywords(domain, during, limit, ts, sentiment, json.dumps(kcount))
        item_exist = SentimentDomainKeywords.query.filter_by(domain=domain, range=during, limit=limit, ts=ts, sentiment=sentiment).first()

        if item_exist:
            db.session.delete(item_exist)
            db.session.add(item)
        else:
            db.session.add(item)

    db.session.commit()


def save_weibos_results(domain, dic, during, limit):
    for k, v in dic.iteritems():
        sentiment = k
        ts, weibos = v

        item = SentimentDomainTopWeibos(domain, during, limit, ts, sentiment, json.dumps(weibos))
        item_exist = SentimentDomainTopWeibos.query.filter_by(domain=domain, range=during, limit=limit, ts=ts, sentiment=sentiment).first()

        if item_exist:
            db.session.delete(item_exist)
            db.session.add(item)
        else:
            db.session.add(item)

    db.session.commit()


def top_weibos(get_results, top=TOP_WEIBOS_LIMIT):
    weibos = []
    for r in get_results():
        weibos.append(r)
    return weibos


def sentiment_field(domain, xapian_search_weibo, start_ts, over_ts, sort_field='reposts_count', save_fields=RESP_ITER_KEYS, during=Hour, w_limit=TOP_WEIBOS_LIMIT, k_limit=TOP_KEYWORDS_LIMIT):
    if domain_uids != []:
        start_ts = int(start_ts)
        over_ts = int(over_ts)

        over_ts = ts2HourlyTime(over_ts, during)
        interval = (over_ts - start_ts) / during

        for i in range(interval, 0, -1):
            emotions_count = {}
            emotions_kcount = {}
            emotions_weibo = {}

            begin_ts = over_ts - during * i
            end_ts = begin_ts + during
            print begin_ts, end_ts, 'domain %s starts calculate' % domain

            query_dict = {
                'timestamp': {'$gt': begin_ts, '$lt': end_ts},
                '$or': []
            }

            for uid in domain_uids:
                query_dict['$or'].append({'user': uid})

            for k, v in emotions_kv.iteritems():
                query_dict['sentiment'] = v
                scount = xapian_search_weibo.search(query=query_dict, count_only=True)
                mset = xapian_search_weibo.search(query=query_dict, sort_by=[sort_field], \
                                                  max_offset=w_limit, mset_direct=True)
                kcount = top_keywords(gen_mset_iter(xapian_search_weibo, mset, fields=['terms']), top=k_limit)
                top_ws = top_weibos(gen_mset_iter(xapian_search_weibo, mset, fields=save_fields), top=w_limit)

                emotions_count[v] = [end_ts, scount]
                emotions_kcount[v] = [end_ts, kcount]
                emotions_weibo[v] = [end_ts, top_ws]

                print k, v, ', emotions count: ', emotions_count, ', emotion keywords length: ', len(kcount), ', emotion weibos length: ', len(top_ws)

            print domain, date, ' %s %s saved emotions counts, keywords and weibos' % (begin_ts, end_ts)
            save_count_results(DOMAIN_LIST.index(domain), emotions_count, during)
            save_kcount_results(DOMAIN_LIST.index(domain), emotions_kcount, during, TOP_KEYWORDS_LIMIT)
            save_weibos_results(DOMAIN_LIST.index(domain), emotions_weibo, during, TOP_WEIBOS_LIMIT)


def cal_field_sentiment_by_date(domainid, datestr, duration):
    start_ts = datetime2ts(datestr)
    end_ts = start_ts + Day
    datestr = datestr.replace('-', '')
    xapian_search_weibo = getXapianWeiboByDate(datestr)
    sentiment_field(domainid, xapian_search_weibo, start_ts=start_ts, over_ts=end_ts, during=duration)


def worker(domainid, datestr):
    print 'domainid:', domainid, 'datestr:', datestr, 'Fifteenminutes: '
    cal_field_sentiment_by_date(domainid, datestr, Fifteenminutes)


if __name__ == '__main__':
    # maintain domains
    # _maintain_domain()

    # test mysql write
    import sys
    # domain = sys.argv[1]
    # date = sys.argv[2] # '2013-09-01'
    domain = 'lawyer'
    domain_uids = DomainUsersFromMysql(domain, TOP_DOMAIN_LIMIT)
    print 'domain uid: ', len(domain_uids)
    for date in ['2013-09-03']:
        worker(domain, date)
