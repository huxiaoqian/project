# -*- coding: utf-8 -*-

import sys
import time
from config import db
from model import SentimentCount, SentimentKeywords, TopWeibos, \
                  SentimentDomainCount, SentimentDomainKeywords, \
                  SentimentDomainTopWeibos, SentimentTopicCount, \
                  SentimentTopicKeywords, SentimentTopicTopWeibos

Fifteenminutes = 15 * 60
keywords_limit = 50
weibos_limit = 50


def main():
    exist_items = db.session.query(SentimentCount)\
                            .filter(SentimentCount.range==Fifteenminutes, \
    	                                 SentimentCount.ts>=begin_ts, \
    	                                 SentimentCount.ts<=end_ts).all()
    for exist_item in exist_items:
    	  db.session.delete(exist_item)
    db.session.commit()
    
    exist_items = db.session.query(SentimentKeywords)\
                            .filter(SentimentKeywords.range==Fifteenminutes, \
    	                                 SentimentKeywords.ts>=begin_ts, \
    	                                 SentimentKeywords.ts<=end_ts, \
    	                                 SentimentKeywords.limit==keywords_limit).all()
    for exist_item in exist_items:
    	  db.session.delete(exist_item)
    db.session.commit()

    exist_items = db.session.query(TopWeibos)\
                            .filter(TopWeibos.range==Fifteenminutes, \
    	                                 TopWeibos.ts>=begin_ts, \
    	                                 TopWeibos.ts<=end_ts, \
    	                                 TopWeibos.limit==weibos_limit).all()
    for exist_item in exist_items:
    	  db.session.delete(exist_item)
    db.session.commit()

    exist_items = db.session.query(SentimentDomainCount)\
                            .filter(SentimentDomainCount.range==Fifteenminutes, \
    	                              SentimentDomainCount.ts>=begin_ts, \
    	                              SentimentDomainCount.ts<=end_ts).all()
    for exist_item in exist_items:
    	  db.session.delete(exist_item)
    db.session.commit()
    
    exist_items = db.session.query(SentimentDomainKeywords)\
                            .filter(SentimentDomainKeywords.range==Fifteenminutes, \
    	                              SentimentDomainKeywords.ts>=begin_ts, \
    	                              SentimentDomainKeywords.ts<=end_ts, \
    	                              SentimentDomainKeywords.limit==keywords_limit).all()
    for exist_item in exist_items:
    	  db.session.delete(exist_item)
    db.session.commit()

    exist_items = db.session.query(SentimentDomainTopWeibos)\
                            .filter(SentimentDomainTopWeibos.range==Fifteenminutes, \
    	                              SentimentDomainTopWeibos.ts>=begin_ts, \
    	                              SentimentDomainTopWeibos.ts<=end_ts, \
    	                              SentimentDomainTopWeibos.limit==weibos_limit).all()
    for exist_item in exist_items:
    	  db.session.delete(exist_item)
    db.session.commit()

    exist_items = db.session.query(SentimentTopicCount)\
                            .filter(SentimentTopicCount.range==Fifteenminutes, \
    	                              SentimentTopicCount.end>=begin_ts, \
    	                              SentimentTopicCount.end<=end_ts).all()
    for exist_item in exist_items:
    	  db.session.delete(exist_item)
    db.session.commit()
    
    exist_items = db.session.query(SentimentTopicKeywords)\
                            .filter(SentimentTopicKeywords.range==Fifteenminutes, \
    	                              SentimentTopicKeywords.end>=begin_ts, \
    	                              SentimentTopicKeywords.end<=end_ts, \
    	                              SentimentTopicKeywords.limit==keywords_limit).all()
    for exist_item in exist_items:
    	  db.session.delete(exist_item)
    db.session.commit()

    exist_items = db.session.query(SentimentTopicTopWeibos)\
                            .filter(SentimentTopicTopWeibos.range==Fifteenminutes, \
    	                              SentimentTopicTopWeibos.end>=begin_ts, \
    	                              SentimentTopicTopWeibos.end<=end_ts, \
    	                              SentimentTopicTopWeibos.limit==weibos_limit).all()
    for exist_item in exist_items:
    	  db.session.delete(exist_item)
    db.session.commit()


if __name__ == '__main__':
	  now_datestr = sys.argv[1]
	  begin_ts = int(time.mktime(time.strptime(now_datestr, '%Y%m%d')))
	  end_ts = begin_ts + 24 * 3600
	  main()
