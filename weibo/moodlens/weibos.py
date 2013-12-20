# -*- coding: utf-8 -*-

import json
import math
import operator
from weibo.extensions import db
from weibo.model import TopWeibos, SentimentDomainTopWeibos, SentimentTopicTopWeibos
from time_utils import datetime2ts


TOP_WEIBOS_LIMIT = 50
TOP_READ = 10

Minute = 60
Fifteenminutes = 15 * Minute
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24
MinInterval = Fifteenminutes


def _top_weibos(weibos_dict, top=TOP_READ):
    results_list = []

    if weibos_dict != {}:
        results = sorted(weibos_dict.iteritems(), key=lambda(k,v): v[0], reverse=False)
        results = results[len(results) - top:]

        for k, v in results:
            results_list.append(v[1])

    return results_list


def parseWeibos(weibos):
    weibo_dict = {}
    weibos = json.loads(weibos)

    for weibo in weibos:
    	_id = weibo['_id']
        reposts_count = weibo['reposts_count']
        weibo_dict[_id] = [reposts_count, weibo]

    return weibo_dict


def search_global_weibos(end_ts, during, sentiment, unit=MinInterval, top=TOP_READ, limit=TOP_WEIBOS_LIMIT, query=None, domain=None):
    weibos_dict = {}
    if during <= unit:
    	upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
    	item = TopWeibos.query.filter(TopWeibos.ts==upbound, \
                                      TopWeibos.sentiment==sentiment, \
                                      TopWeibos.range==unit, \
                                      TopWeibos.limit==top).first()
        if item:
        	weibos_dict = parseWeibos(item.weibos)

    else:
        start_ts = end_ts - during
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        lowbound = (start_ts / unit) * unit
        items = TopWeibos.query.filter(TopWeibos.ts>lowbound, \
                                       TopWeibos.ts<=upbound, \
                                       TopWeibos.sentiment==sentiment, \
                                       TopWeibos.range==unit, \
                                       TopWeibos.limit==top).all()

        for item in items:
            weibo_dict = parseWeibos(item.weibos)
            for k, v in weibo_dict.iteritems():
                try:
                    weibos_dict[k] += v
                except KeyError:
                    weibos_dict[k] = v

    weibos_list = _top_weibos(weibos_dict, top)

    return weibos_list


def search_topic_weibos(end_ts, during, sentiment, unit=MinInterval, top=TOP_READ, limit=TOP_WEIBOS_LIMIT, query=None, domain=None):
    weibos_dict = {}
    if during <= unit:
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        item = db.session.query(SentimentTopicTopWeibos).filter(SentimentTopicTopWeibos.ts==upbound, \
                                                  SentimentTopicTopWeibos.sentiment==sentiment, \
                                                  SentimentTopicTopWeibos.range==unit, \
                                                  SentimentTopicTopWeibos.query==query, \
                                                  SentimentTopicTopWeibos.limit==top).first()
        if item:
            weibos_dict = parseWeibos(item.weibos)

    else:
        start_ts = end_ts - during
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        lowbound = (start_ts / unit) * unit
        items = db.session.query(SentimentTopicTopWeibos).filter(SentimentTopicTopWeibos.end>lowbound, \
                                                   SentimentTopicTopWeibos.end<=upbound, \
                                                   SentimentTopicTopWeibos.sentiment==sentiment, \
                                                   SentimentTopicTopWeibos.range==unit, \
                                                   SentimentTopicTopWeibos.query==query, \
                                                   SentimentTopicTopWeibos.limit==top).all()
        for item in items:
            weibo_dict = parseWeibos(item.weibos)
            for k, v in weibo_dict.iteritems():
                try:
                    weibos_dict[k] += v
                except KeyError:
                    weibos_dict[k] = v

    weibos_dict = _top_weibos(weibos_dict, top)

    return weibos_dict


def search_domain_weibos(end_ts, during, sentiment, unit=MinInterval, top=TOP_READ, limit=TOP_WEIBOS_LIMIT, query=None, domain=None):
    weibos_dict = {}
    if during <= unit:
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        item = SentimentDomainTopWeibos.query.filter(SentimentDomainTopWeibos.ts==upbound, \
                                                     SentimentDomainTopWeibos.sentiment==sentiment, \
                                                     SentimentDomainTopWeibos.range==unit, \
                                                     SentimentDomainTopWeibos.domain==domain, \
                                                     SentimentDomainTopWeibos.limit==top).first()
        if item:
            weibos_dict = parseWeibos(item.weibos)

    else:
        start_ts = end_ts - during
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        lowbound = (start_ts / unit) * unit
        items = SentimentDomainTopWeibos.query.filter(SentimentDomainTopWeibos.ts>lowbound, \
                                                      SentimentDomainTopWeibos.ts<=upbound, \
                                                      SentimentDomainTopWeibos.sentiment==sentiment, \
                                                      SentimentDomainTopWeibos.range==unit, \
                                                      SentimentDomainTopWeibos.domain==domain, \
                                                      SentimentDomainTopWeibos.limit==top).all()

        for item in items:
            weibo_dict = parseWeibos(item.weibos)
            for k, v in weibo_dict.iteritems():
                try:
                    weibos_dict[k] += v
                except KeyError:
                    weibos_dict[k] = v

    weibos_dict = _top_weibos(weibos_dict, top)

    return weibos_dict


if __name__ == '__main__':
    emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
    end_ts = datetime2ts('2013-09-01')
    during = 5 * Day

    for k, v in emotions_kv.iteritems():
        print v
        weibos = search_global_weibos(end_ts, during, v)
        for key, value in weibos.iteritems():
            print key, value
            
    print 'topic'
    for k, v in emotions_kv.iteritems():
        print v
        weibos = search_topic_weibos(end_ts, during, v, query='钓鱼岛')
        for key, value in weibos.iteritems():
            print key, value

    
    print 'domain'
    for k, v in emotions_kv.iteritems():          
        weibos = search_domain_weibos(end_ts, during, v, domain=1)
        for key, value in weibos.iteritems():
            print key, value
