# -*- coding: utf-8 -*-


import json
import math
import operator
from sqlalchemy import func
from weibo.extensions import db
from time_utils import datetime2ts
from weibo.model import SentimentCount, SentimentDomainCount, SentimentTopicCount, SentimentRtTopicCount


Minute = 60
Fifteenminutes = 15 * Minute
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24
MinInterval = Fifteenminutes


def search_global_counts(end_ts, during, sentiment, unit=MinInterval, query=None, domain=None, customized='1'):
    if during <= unit:
    	upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
    	item = SentimentCount.query.filter(SentimentCount.ts==upbound, \
                                           SentimentCount.sentiment==sentiment, \
                                           SentimentCount.range==unit).first()
        if item:
        	count = [end_ts * 1000, item.count]
        else:
        	count = [end_ts * 1000, 0]

    else:
        start_ts = end_ts - during
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        lowbound = (start_ts / unit) * unit
        count = db.session.query(func.sum(SentimentCount.count)).filter(SentimentCount.ts>lowbound, \
                                            SentimentCount.ts<=upbound, \
                                            SentimentCount.sentiment==sentiment, \
                                            SentimentCount.range==unit).all()
        
        if count and count[0] and count[0][0]:
            count = [end_ts * 1000, int(count[0][0])]
        else:
            count = [end_ts * 1000, 0]

    return count


def search_topic_counts(end_ts, during, sentiment, unit=MinInterval, query=None, domain=None, customized='1'):
    if during <= unit:
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        if customized == '0':
            print 'here'
            item = db.session.query(SentimentRtTopicCount).filter(SentimentRtTopicCount.end==upbound, \
                                              SentimentRtTopicCount.sentiment==sentiment, \
                                              SentimentRtTopicCount.range==unit, \
                                              SentimentRtTopicCount.query==query).first()
        else:
            item = db.session.query(SentimentTopicCount).filter(SentimentTopicCount.end==upbound, \
                                              SentimentTopicCount.sentiment==sentiment, \
                                              SentimentTopicCount.range==unit, \
                                              SentimentTopicCount.query==query).first()
        if item:
        	count = [end_ts * 1000, item.count]
        else:
        	count = [end_ts * 1000, 0]

    else:
        start_ts = end_ts - during
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        lowbound = (start_ts / unit) * unit
        if customized == '0':
            count = db.session.query(func.sum(SentimentRtTopicCount.count)).filter(SentimentRtTopicCount.end>lowbound, \
                                            SentimentRtTopicCount.end<=upbound, \
                                            SentimentRtTopicCount.sentiment==sentiment, \
                                            SentimentRtTopicCount.range==unit, \
                                            SentimentRtTopicCount.query==query).all()

        else:
            count = db.session.query(func.sum(SentimentTopicCount.count)).filter(SentimentTopicCount.end>lowbound, \
                                                SentimentTopicCount.end<=upbound, \
                                                SentimentTopicCount.sentiment==sentiment, \
                                                SentimentTopicCount.range==unit, \
                                                SentimentTopicCount.query==query).all()

        if count and count[0] and count[0][0]:
            count = [end_ts * 1000, int(count[0][0])]
        else:
            count = [end_ts * 1000, 0]

    return count


def search_domain_counts(end_ts, during, sentiment, unit=MinInterval, query=None, domain=None, customized='1'):
    if during <= unit:
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        item = SentimentDomainCount.query.filter(SentimentDomainCount.ts==upbound, \
                                              SentimentDomainCount.sentiment==sentiment, \
                                              SentimentDomainCount.range==unit, \
                                              SentimentDomainCount.domain==domain).first()
        if item:
        	count = [end_ts * 1000, item.count]
        else:
        	count = [end_ts * 1000, 0]

    else:
        start_ts = end_ts - during
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        lowbound = (start_ts / unit) * unit
        count = db.session.query(func.sum(SentimentDomainCount.count)).filter(SentimentDomainCount.ts>lowbound, \
                                                                              SentimentDomainCount.ts<=upbound, \
                                                                              SentimentDomainCount.sentiment==sentiment, \
                                                                              SentimentDomainCount.range==unit, \
                                                                              SentimentDomainCount.domain==domain).all()

        if count and count[0] and count[0][0]:
            count = [end_ts * 1000, int(count[0][0])]
        else:
            count = [end_ts * 1000, 0]

    return count


if __name__ == '__main__':
    emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
    end_ts = datetime2ts('2013-09-01')
    during = 5 * Day

    for k, v in emotions_kv.iteritems():
        count = search_domain_counts(end_ts, during, v, domain=0)
