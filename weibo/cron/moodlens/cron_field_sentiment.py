# -*- coding: utf-8 -*-


import json
from config import cron_start, cron_end, xapian_search_weibo, \
                   xapian_search_domain, xapian_search_user, emotions_kv
from time_utils import datetime2ts, ts2HourlyTime
from xapian_weibo.utils import top_keywords
from config import db
from model import SentimentDomainCount, SentimentDomainKeywords, \
                  SentimentDomainTopWeibos, Domain
from sqlalchemy.exc import IntegrityError
from dynamic_xapian_weibo import getXapianWeiboByDate
import multiprocessing


Minute = 60
Fifteenminutes = 15 * 60
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24

start_range_ts = datetime2ts(cron_start)
end_range_ts = datetime2ts(cron_end)

TOP_KEYWORDS_LIMIT = 50
TOP_WEIBOS_LIMIT = 50
TOP_DOMAIN_LIMIT = 10000

RESP_ITER_KEYS = ['_id', 'user', 'retweeted_uid', 'retweeted_mid', 'text', 'timestamp', 'reposts_count', 'bmiddle_pic', 'geo', 'comments_count', 'sentiment', 'terms']
FIELDS_VALUE = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology', 'oversea']
FIELDS_ZH_NAME = [u'文化', u'教育', u'娱乐', u'时尚', u'财经', u'媒体', u'体育', u'科技', u'海外']
FIELDS2ID = {}
FIELDS2ZHNAME = {}

for key in FIELDS_VALUE:
    idx = FIELDS_VALUE.index(key)
    FIELDS2ID[key] = idx
    FIELDS2ZHNAME[key] = FIELDS_ZH_NAME[idx]


def _is_domain_active(domain):
    domain = Domain.query.filter_by(idx=domain).first()
    if domain:
        return domain.active
    else:
        return False


def _domains_active():
    domains = []
    items = Domain.query.filter_by(active=True).all()

    for item in items:
        domains.append({'idx': item.idx, 'name': item.name, 'zhname': item.zhname})

    return domains


def _domain_id(domain):
    domain = Domain.query.filter_by(name=domain)
    if domain:
        return domain.id
    else:
        return None


def _maintain_domain():
    for name in FIELDS_VALUE:
        idx = FIELDS2ID[name]
        zhname = FIELDS2ZHNAME[name]
        active = True

        item = Domain(idx, name, zhname, active)
        item_exist = Domain.query.filter_by(idx=idx, name=name, zhname=zhname).first()

        if item_exist:
            db.session.delete(item_exist)
            db.session.add(item)
        else:
            db.session.add(item)

    db.session.commit()
    count = Domain.query.count()

    print 'maintain domain over, there is %s domains now' % count


def getDomainUsers(domain, top=TOP_DOMAIN_LIMIT):
    active = _is_domain_active(domain)
    domain_id = domain
    domain_uids = []

    if not active or not domain_id:
        return []

    if domain != 'oversea':
        count, get_results = xapian_search_domain.search(query={'domain': str(domain_id)}, sort_by=['followers_count'], fields=['_id'], max_offset=top)
    else:
        count, get_results = xapian_search_user.search(query={'location': FIELDS2ZHNAME('oversea')}, sort_by=['followers_count'], fields=['_id'], max_offset=top)

    for user in get_results():
        domain_uids.append(user['_id'])

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


def read_count_results(domain, ts, sentiment, range=Hour):
    item_exist = SentimentCount.query.filter_by(range=range, ts=ts, sentiment=sentiment).first()
    if item_exist:
        return item_exist.count
    else:
        return 0


def top_weibos(get_results, top=TOP_WEIBOS_LIMIT):
    weibos = []
    for r in get_results():
        weibos.append(r)
    return weibos


def sentiment_field(domain, xapian_search_weibo=xapian_search_weibo, start_ts=start_range_ts, over_ts=end_range_ts, sort_field='reposts_count', save_fields=RESP_ITER_KEYS, during=Hour, w_limit=TOP_WEIBOS_LIMIT, k_limit=TOP_KEYWORDS_LIMIT):
    domain_uids = getDomainUsers(domain)
    print len(domain_uids)

    if domain_uids != []:
        start_ts = int(start_ts)
        over_ts = int(over_ts)

        over_ts = ts2HourlyTime(over_ts, during)
        interval = (over_ts - start_ts) / during

        for i in range(interval):
            emotions_count = {}
            emotions_kcount = {}
            emotions_weibo = {}

            end_ts = over_ts - during * i
            begin_ts = end_ts - during

            query_dict = {
                'timestamp': {'$gt': begin_ts, '$lt': end_ts},
                '$or': []
            }

            for uid in domain_uids:
                query_dict['$or'].append({'user': uid})

            for k, v in emotions_kv.iteritems():
                query_dict['sentiment'] = v
                scount = xapian_search_weibo.search(query=query_dict, count_only=True)
                count, get_results = xapian_search_weibo.search(query=query_dict, fields=save_fields, \
                                                                sort_by=[sort_field], max_offset=w_limit)
                count, get_results_1 = xapian_search_weibo.search(query=query_dict, fields=save_fields, \
                                                                sort_by=[sort_field], max_offset=w_limit)
                kcount = top_keywords(get_results, top=k_limit)
                top_ws = top_weibos(get_results_1, top=w_limit)

                emotions_count[v] = [end_ts, scount]
                emotions_kcount[v] = [end_ts, kcount]
                emotions_weibo[v] = [end_ts, top_ws]

            print 'saved emotions count, keywords and weibos'
            save_count_results(domain, emotions_count, during)
            save_kcount_results(domain, emotions_kcount, during, TOP_KEYWORDS_LIMIT)
            save_weibos_results(domain, emotions_weibo, during, TOP_WEIBOS_LIMIT)


def cal_field_sentiment_by_date(domainid, datestr, duration):
    start_ts = datetime2ts(datestr)
    end_ts = start_ts + Day
    datestr = datestr.replace('-', '')
    xapian_search_weibo = getXapianWeiboByDate(datestr)
    sentiment_field(domainid, xapian_search_weibo, start_ts=start_ts, over_ts=end_ts, during=duration)


def worker(domainid, datestr):
    print 'domainid:', domainid
    print 'datestr:', datestr
    print 'Fifteenminutes: '
    cal_field_sentiment_by_date(domainid, datestr, Fifteenminutes)
    print 'Day'
    cal_field_sentiment_by_date(domainid, datestr, Day)


if __name__ == '__main__':
    # maintain domains
    # _maintain_domain()

    # test mysql write
    domains = _domains_active()
    jobs = []
    for datestr in ['2013-09-01', '2013-09-02', '2013-09-03', '2013-09-04', '2013-09-05']:
        for domain in domains:
            p = multiprocessing.Process(target=worker, args=(domain['idx'], datestr))
            jobs.append(p)
            p.start()

    # test mysql read
    # start_range_ts = datetime2ts('2013-09-29')
    # end_range_ts = datetime2ts('2013-10-03')
    # test_read_count_results(start_range_ts, end_range_ts)
