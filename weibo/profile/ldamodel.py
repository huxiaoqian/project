#-*-coding:utf-8-*-
import sys
import gensim
from gensim import corpora, models, similarities
import json
import time
import datetime
from operator import itemgetter, attrgetter
from time_utils import datetime2ts

from weibo.global_config import xapian_search_weibo

##提取已保存的模型
dictionary = gensim.corpora.Dictionary.load('weibo/profile/ldamodel/100000U2M50T.dict')
lda = gensim.models.ldamodel.LdaModel.load('weibo/profile/ldamodel/100000U2M50T.lda')

def search(uid, start_str, end_str):
    f = open('weibo/profile/data/stpwds_linhao_20130514.txt', 'r')
    swds = set()
    for line in f.readlines():
       word = line.strip().split(' ')[0]
       swds.add(word)
    startts = datetime2ts(start_str)
    endts = datetime2ts(end_str)
    query_dict = {
    	'timestamp':{
            '$gt': startts, 
            '$lt': endts
    	},
    	'user': uid
    }
    count, get_results = xapian_search_weibo.search(query=query_dict, fields=['user', 'terms','retweeted_status'])

    u_kwds = {}
    if count:
    	kwdlst=[]
    	for r in get_results():
    	    for t in r['terms']:
    	       	if t not in swds:
                    if r['terms'][t]>1:
                        for i in range(0,r['terms'][t]):
    		              kwdlst.append(t)
    		    else:
    		        kwdlst.append(t)
    		else:
    		    continue
            ###对转发的微博提取原微博
            if r['retweeted_status'] is not None:
                rc, r_results = xapian_search_weibo.search(query={'_id': r['retweeted_status']},fields=['user', 'terms'])
                for rr in r_results():
                    for t1 in rr['terms']:
                        if t1 not in swds:
                            if rr['terms'][t1]>1:
                                for i in range(0,rr['terms'][t1]):
                                    kwdlst.append(t1)
                            else:
                                kwdlst.append(t1)
                        else:
                            continue
    	u_kwds[uid] = kwdlst
        return u_kwds
    else:
    	return None

def lda_topic(uid, start_str, end_str):
    u_kwds = search(uid, start_str, end_str)
    docs = []
    if u_kwds:
        for u in u_kwds:
            docs.append(u_kwds[u])
    for doc in docs:
        toVector = dictionary.doc2bow(doc)
        if len(toVector)>0:
            doc_lda = lda[toVector]
            #sorted_topics_prob = sorted(doc_lda, key=itemgetter(1), reverse=True)
            sorted_topics_prob = doc_lda
            keywords = []
            for topic, prob in sorted_topics_prob:
                keyword_prob = lda.show_topic(topic, topn=10)
                keywords_10_result  = []
                for kwd, kprob in keyword_prob:
                    keywords_10_result.append([kwd, kprob])
                keywords.append(keywords_10_result)
            return {'topics': sorted_topics_prob, 'keywords': keywords}

if __name__ == '__main__':
    lda_topic(1813080181, '2013-03-01', '2013-05-01')
