# -*- coding: utf-8 -*-


import json
import math
import operator
from weibo.extensions import db
from weibo.model import SentimentKeywords, SentimentTopicKeywords, \
                        SentimentDomainKeywords, SentimentRtTopicKeywords
from time_utils import datetime2ts


TOP_KEYWORDS_LIMIT = 50
TOP_READ = 10

Minute = 60
Fifteenminutes = 15 * Minute
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24
MinInterval = Fifteenminutes


def parseKcount(kcount):
    kcount_dict = {}
    kcount = json.loads(kcount)  #将kcount这个json文件转化为字典

    for k, v in kcount:
    	kcount_dict[k] = v

    return kcount_dict


def _top_keywords(kcount_dict, top=TOP_READ):
    results_dict = {}

    if kcount_dict != {}:  #排序，key为选择元素的哪一项进行排序，reverse默认false为升序排列
        results = sorted(kcount_dict.iteritems(), key=operator.itemgetter(1), reverse=False)
        results = results[len(results) - top:]

        for k, v in results:
            results_dict[k] = v

    return results_dict


def search_global_keywords(end_ts, during, sentiment, unit=MinInterval, top=TOP_READ, limit=TOP_KEYWORDS_LIMIT, query=None, domain=None, customized='1'):
    kcounts_dict = {}
    if during <= unit:
    	upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
    	item = SentimentKeywords.query.filter(SentimentKeywords.ts==upbound, \
                                              SentimentKeywords.sentiment==sentiment, \
                                              SentimentKeywords.range==unit, \
                                              SentimentKeywords.limit==limit).first()
        if item:
        	kcounts_dict = parseKcount(item.kcount)

    else:
        start_ts = end_ts - during
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        lowbound = (start_ts / unit) * unit   #将查询时间转换到15分钟的倍数
        items = SentimentKeywords.query.filter(SentimentKeywords.ts>lowbound, \
                                               SentimentKeywords.ts<=upbound, \
                                               SentimentKeywords.sentiment==sentiment, \
                                               SentimentKeywords.range==unit, \
                                               SentimentKeywords.limit==limit).all()
#？？？！！query.filter的返回值是什么类型？
        for item in items:
            kcount_dict = parseKcount(item.kcount) 
            for k, v in kcount_dict.iteritems():
                try:
                    kcounts_dict[k] += v
                except KeyError:
                    kcounts_dict[k] = v

    kcounts_dict = _top_keywords(kcounts_dict, top)

    return kcounts_dict


def search_topic_keywords(end_ts, during, sentiment, unit=MinInterval, top=TOP_READ, limit=TOP_KEYWORDS_LIMIT, query=None, domain=None, customized='1'):
    kcounts_dict = {}
    if during <= unit:
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        if customized == '0':
          item = db.session.query(SentimentRtTopicKeywords).filter(SentimentRtTopicKeywords.end==upbound, \
                                              SentimentRtTopicKeywords.sentiment==sentiment, \
                                              SentimentRtTopicKeywords.range==unit, \
                                              SentimentRtTopicKeywords.query==query, \
                                              SentimentRtTopicKeywords.limit==limit).first()
        else:
          item = db.session.query(SentimentTopicKeywords).filter(SentimentTopicKeywords.end==upbound, \
                                              SentimentTopicKeywords.sentiment==sentiment, \
                                              SentimentTopicKeywords.range==unit, \
                                              SentimentTopicKeywords.query==query, \
                                              SentimentTopicKeywords.limit==limit).first()
        if item:
            kcounts_dict = parseKcount(item.kcount)

    else:
        start_ts = end_ts - during
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        lowbound = (start_ts / unit) * unit
        if customized == '0':
          items = db.session.query(SentimentRtTopicKeywords).filter(SentimentRtTopicKeywords.end>lowbound, \
                                                 SentimentRtTopicKeywords.end<=upbound, \
                                                 SentimentRtTopicKeywords.sentiment==sentiment, \
                                                 SentimentRtTopicKeywords.range==unit, \
                                                 SentimentRtTopicKeywords.query==query, \
                                                 SentimentRtTopicKeywords.limit==limit).all()
        else:
          items = db.session.query(SentimentTopicKeywords).filter(SentimentTopicKeywords.end>lowbound, \
                                                 SentimentTopicKeywords.end<=upbound, \
                                                 SentimentTopicKeywords.sentiment==sentiment, \
                                                 SentimentTopicKeywords.range==unit, \
                                                 SentimentTopicKeywords.query==query, \
                                                 SentimentTopicKeywords.limit==limit).all()
        for item in items:
            kcount_dict = parseKcount(item.kcount)
            for k, v in kcount_dict.iteritems():
                try:
                    kcounts_dict[k] += v
                except KeyError:
                    kcounts_dict[k] = v

    kcounts_dict = _top_keywords(kcounts_dict, top)

    return kcounts_dict


def search_domain_keywords(end_ts, during, sentiment, unit=MinInterval, top=TOP_READ, limit=TOP_KEYWORDS_LIMIT, query=None, domain=None, customized='1'):
    kcounts_dict = {}
    if during <= unit:
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        item = SentimentDomainKeywords.query.filter(SentimentDomainKeywords.ts==upbound, \
                                              SentimentDomainKeywords.sentiment==sentiment, \
                                              SentimentDomainKeywords.range==unit, \
                                              SentimentDomainKeywords.domain==domain, \
                                              SentimentDomainKeywords.limit==limit).first()
        if item:
            kcounts_dict = parseKcount(item.kcount)

    else:
        start_ts = end_ts - during
        upbound = int(math.ceil(end_ts / (unit * 1.0)) * unit)
        lowbound = (start_ts / unit) * unit
        items = SentimentDomainKeywords.query.filter(SentimentDomainKeywords.ts>lowbound, \
                                               SentimentDomainKeywords.ts<=upbound, \
                                               SentimentDomainKeywords.sentiment==sentiment, \
                                               SentimentDomainKeywords.range==unit, \
                                               SentimentDomainKeywords.domain==domain, \
                                               SentimentDomainKeywords.limit==limit).all()

        for item in items:
            kcount_dict = parseKcount(item.kcount)
            for k, v in kcount_dict.iteritems():
                try:
                    kcounts_dict[k] += v
                except KeyError:
                    kcounts_dict[k] = v

    kcounts_dict = _top_keywords(kcounts_dict, top)

    return kcounts_dict


if __name__ == '__main__':
    emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
    end_ts = datetime2ts('2013-09-01')
    during = 5 * Day

    for k, v in emotions_kv.iteritems():
        print v
        keywords_with_count = search_global_keywords(end_ts, during, v)
        for key, value in keywords_with_count.iteritems():
            print key, value
            
    print 'topic'
    for k, v in emotions_kv.iteritems():
        print v
        keywords_with_count = search_topic_keywords(end_ts, during, v, query='钓鱼岛')
        for key, value in keywords_with_count.iteritems():
            print key, value

    
    print 'domain'
    for k, v in emotions_kv.iteritems():          
        keywords_with_count = search_domain_keywords(end_ts, during, v, domain=1)
        for key, value in keywords_with_count.iteritems():
            print key, value
