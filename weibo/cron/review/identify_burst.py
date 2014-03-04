# -*- coding: utf-8 -*-

from config import db
from model import BurstIdentification

TOPK = 1000

class TopkHeap(object):
    def __init__(self, k):
        self.k = k
        self.data = []
 
    def Push(self, elem):
        if len(self.data) < self.k:
            heapq.heappush(self.data, elem)
        else:
            topk_small = self.data[0][0]
            if elem[0] > topk_small:
                heapq.heapreplace(self.data, elem)
 
    def TopK(self):
        return [x for x in reversed([heapq.heappop(self.data) for x in xrange(len(self.data))])]


def void_leveldb(ldb):
    count = 0
    for k, v in ldb.RangeIter():
        count += 1
    
    if count > 0:
        return False
    else:
        return True


def burst_rank(topk=TOPK, identifyWindow=1):
    # 突发排序
    print 'burst rank'
    previous_exist = True
    if void_leveldb(global_leveldb):
        return

    if void_leveldb(global_previous_leveldb):
        previous_exist = False

    burst_active_th = TopkHeap(topk)
    burst_important_th = TopkHeap(topk)

    count = 0
    ts = te = time.time() 
    for key, value in global_leveldb.RangeIter():
        active, important, follower, domain = value.split('_')
        uid = int(key)
        active = int(active)
        important = int(important)
        follower = int(follower)
        domain = int(domain)

        # 突发排序
        if previous_exist:
            try:
                value = global_previous_leveldb.Get(str(key))
                previous_active, previous_important, previous_follower, previous_domain = value.split('_')
            except KeyError:
                previous_active = previous_important = 0
            
            diff_active = active - int(previous_active)
            diff_important = important - int(previous_important)
            burst_active_th.Push((diff_active, diff_important, active, important, follower, uid))
            burst_important_th.Push((diff_important, diff_active, active, important, follower, uid))

        if count % 10000 == 0:
            te = time.time()
            print 'iter rank ', count, '%s sec' % (te - ts)
            ts = te
        count += 1

    save_burst(burst_active_th.TopK(), 'active')
    save_burst(burst_important_th.TopK(), 'important')


def save_burst(data, method="active", identifyWindow=1):
	# delete old data
	if data and len(data):
        exist_items = db.session.query(BurstIdentification).\
                                filter(BurstIdentification.identifyDate == now_datestr, \
                                       BurstIdentification.identifyWindow==identifyWindow, \
                                       BurstIdentification.identifyMethod==method).all()
        for exist_item in exist_items:
            db.session.delete(exist_item)
        db.session.commit()

	# add new data
    for i, tuples in enumerate(data):
        rank = i + 1
        if method == 'active':
            diff_active, diff_important, active, important, follower, uid = tuples
        elif method == 'important':
            diff_important, diff_active, active, important, follower, uid = tuples

        print 'burst save: ', rank, diff_active, diff_important, follower, active, important, now_datestr, identifyWindow, method
        new_item = BurstIdentification(rank, uid, follower, active, important, diff_active, diff_important, now_datestr, identifyWindow, method)
        db.session.add(new_item)
        db.session.commit()

if __name__ == '__main__':
	# get datestr
    now_datestr = sys.argv[1] # '20130901'
    before_datestr = sys.argv[2] # '20130830'

	# identify rank
    global_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s' % now_datestr),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    global_previous_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s' % before_datestr),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    burst_rank()
