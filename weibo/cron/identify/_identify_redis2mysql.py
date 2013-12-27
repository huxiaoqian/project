# -*- coding: utf-8 -*-

import redis
import heapq
import datetime
from model import WholeIdentification
from config import REDIS_HOST, REDIS_PORT, fields_value, labels, db

DOMAINS_LIST = fields_value + labels
USER_DOMAIN = "user_domain" # user domain hash,
GLOBAL_ACTIVE_COUNT = "global_active_%s" # date as '20131227',
GLOBAL_IMPORTANT_COUNT = "global_important_%s" # date as '20131227',
DOMAIN_ACTIVE_COUNT = "domain_active_%s:%s" # date as '20131227', domain
DOMAIN_IMPORTANT_COUNT = "domain_important_%s:%s" # date as '20131227', domain
CAL_TOPK = 1000
SAVE_TOPK = 500


class TopkHeap(object):
    def __init__(self, k):
        self.k = k
        self.data = []
 
    def Push(self, elem):
    	# elem: (score, uid, otherscore) tuple
        if len(self.data) < self.k:
            heapq.heappush(self.data, elem)
        else:
            topk_small = self.data[0][0]
            if elem[0] > topk_small:
                heapq.heapreplace(self.data, elem)
 
    def TopK(self):
        return [x for x in reversed([heapq.heappop(self.data) for x in xrange(len(self.data))])]


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def get_now_datestr():
    datestr = datetime.datetime.utcnow().strftime("%Y%m%d") # 20131227
    return datestr


def getUserActive(r, uid):
    active = r.hget(GLOBAL_ACTIVE_COUNT % now_datestr, str(uid))
    if not active:
        return -1
    
    return int(active)


def getUserImportant(r, uid):
    important = r.hget(GLOBAL_IMPORTANT_COUNT % now_datestr, str(uid))
    if not important:
        return -1
    
    return int(important)


def getUserFollowerscount(uid):
    return -1


def save_topK_results(data, now_datestr, identifyMethod='important', identifyWindow=1, domainid=-1):
    for score, uid, otherscore in data:
        followersCount = getUserFollowerscount(uid)
        if identifyMethod == 'important':
            importantCount = score
            activeCount = otherscore
        if identifyMethod == 'active':
            activeCount = score
            importantCount = otherscore

        item = WholeIdentification(rank, uid, followersCount, activeCount, importantCount, identifyDate, identifyWindow, identifyMethod)
        item_exist = db.session.query(WholeIdentification).filter_by(WholeIdentification.identifyMethod==identifyMethod, \
                                                                     WholeIdentification.identifyWindow==identifyWindow, \
                                                                     WholeIdentification.userId==uid, \
                                                                     WholeIdentification.identifyDate==now_datestr)
        if item_exist:
            db.session.delete(item_exist)
        db.session.add(item)
        db.session.commit()    


def identify_whole_rank(r, topk=CAL_TOPK):
    # active rank
    theap = TopkHeap(topk)
    next_cursor, user_dicts = r.hscan(GLOBAL_ACTIVE_COUNT % now_datestr)
    
    while next_cursor != '0':
    	for uid, score in user_dicts.iteritems():
    	    uid = int(uid)
            score = int(score)
            otherscore = getUserImportant(r, uid)
            theap.Push((score, uid, otherscore))

        next_cursor, user_dicts = r.hscan(GLOBAL_ACTIVE_COUNT % now_datestr, next_cursor)

    active_topK = theap.TopK()
    print 'whole active rank: ', len(active_topK)
    save_topK_results(active_topK, now_datestr, 'active')

    # important rank
    theap = TopkHeap(topk)
    next_cursor, user_dicts = r.hscan(GLOBAL_IMPORTANT_COUNT % now_datestr)
    
    while next_cursor != '0':
    	for uid, score in user_dicts.iteritems():
            uid = int(uid)
    	    score = int(score)
            otherscore = getUserActive(r, uid)
    	    theap.Push((score, uid, otherscore))

        next_cursor, user_dicts = r.hscan(GLOBAL_IMPORTANT_COUNT % now_datestr, next_cursor)

    important_topK = theap.TopK()
    print 'whole important rank: ', len(important_topK)
    save_topK_results(important_topK, now_datestr, 'important')
    

def identify_domain_rank(r, domainid, topk=CAL_TOPK):
    # active rank
    theap = TopkHeap(topk)
    next_cursor, user_dicts = r.hscan(DOMAIN_ACTIVE_COUNT % (now_datestr, domainid))
    
    while next_cursor != '0':
        for uid, score in user_dicts.iteritems():
            uid = int(uid)
            score = int(score)
            otherscore = getUserImportant(r, uid)
            theap.Push((score, uid, otherscore))

        next_cursor, user_dicts = r.hscan(DOMAIN_ACTIVE_COUNT % (now_datestr, domainid), next_cursor)

    active_topK = theap.TopK()
    print domainid, ' save active rank ', len(active_topK)
    #save_topK_results(active_topK, now_datestr, 'domain', domainid=domainid)

    # important rank
    theap = TopkHeap(topk)
    next_cursor, user_dicts = r.hscan(DOMAIN_IMPORTANT_COUNT % (now_datestr, domainid))
    
    while next_cursor != '0':
        for uid, score in user_dicts.iteritems():
            uid = int(uid)
            score = int(score)
            otherscore = getUserActive(r, uid)
            theap.Push((score, uid, otherscore))

        next_cursor, user_dicts = r.hscan(DOMAIN_IMPORTANT_COUNT % (now_datestr, domainid), next_cursor)

    important_topK = theap.TopK()
    print domainid, ' save important rank ', len(important_topK)
    #save_topK_results(important_topK, now_datestr, 'domain', domainid=domainid)


def identify_burst_rank(r):
    pass
        

if __name__ == '__main__':
    #now_datestr = get_now_datestr()
    now_datestr = '20131227'
    r = _default_redis()

    identify_whole_rank(r)
    for domain in DOMAINS_LIST:
        domainid = DOMAINS_LIST.index(domain)
        identify_domain_rank(r, domainid)