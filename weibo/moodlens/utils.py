# -*- coding: utf-8 -*-

import json
import numpy as np
from weibo.extensions import db
from weibo.model import SentimentCount, SentimentKeywords, TopWeibos, \
                        SentimentTopicCount, SentimentTopicKeywords, \
                        SentimentTopicTopWeibos
import os
import time
import datetime
import operator
from time_utils import datetime2ts
from weibo.global_config import xapian_search_weibo, xapian_search_user, \
                                cron_start, cron_end, emotions_kv

from sqlalchemy import func


Minute = 60
Fifteenminutes = 15 * Minute
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24
MinInterval = Fifteenminutes

TOP_KEYWORDS_LIMIT = 50
TOP_WEIBOS_LIMIT = 50

start_range_ts = datetime2ts(cron_start)
end_range_ts = datetime2ts(cron_end)
ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te - ts)
        return result
    return timed


def ts2HourlyTime(ts, interval):
    # interval 取 Minite、Hour

    ts = ts - ts % interval
    return ts


def read_count_results(sentiment, start_ts=start_range_ts, over_ts=end_range_ts, during=Hour):
    if during <= MinInterval:
        item_exist = SentimentCount.query.filter_by(ts=over_ts, \
                                                    sentiment=sentiment, \
                                                    range=MinInterval).first()
        if item_exist:
            return item_exist.count
        else:
            return 0
    else:
        count = db.session.query(func.sum(SentimentCount.count)).filter(SentimentCount.ts > start_ts, \
                                                                        SentimentCount.ts < over_ts, \
                                                                        SentimentCount.sentiment==sentiment, \
                                                                        SentimentCount.range==MinInterval)
                                                               
        return int(count[0][0])


def read_kcount_results(sentiment, start_ts=start_range_ts, over_ts=end_range_ts, during=Hour):
    if during <= MinInterval:
        item_exist = SentimentKeywords.query.filter_by(ts=over_ts, \
                                                       sentiment=sentiment, \
                                                       range=MinInterval).first()
        if item_exist:
            return item_exist.kcount
        else:
            return {}
    else:
        kcounts_dict = {}
        kcounts = SentimentKeywords.query.filter(SentimentKeywords.ts>start_ts, \
                                                    SentimentKeywords.ts<over_ts, \
                                                    SentimentKeywords.sentiment==sentiment, \
                                                    SentimentKeywords.range==MinInterval).all()
        for kcount in kcounts:
            k_c = json.loads(kcount.kcount)
            for k, v in k_c:
                try:
                    kcounts_dict[k] += v
                except KeyError:
                    kcounts_dict[k] = v            

        return list(kcounts_dict)


def read_weibo_results(sentiment, start_ts=start_range_ts, over_ts=end_range_ts, during=Hour):
    if during <= MinInterval:
        weibos = []
        item_exist = TopWeibos.query.filter_by(ts=over_ts, \
                                               sentiment=sentiment, \
                                               range=MinInterval).all()
        for item in item_exist:
            weibos.extend(json.loads(item.weibos))

        return weibos

    else:
        weibos = db.session.query.filter(TopWeibos.ts > start_ts, \
                                         TopWeibos.ts < over_ts, \
                                         TopWeibos.sentiment==sentiment, \
                                         TopWeibos.range==MinInterval)

        return list(weibos)


def read_range_weibos_results(start_ts=start_range_ts, over_ts=end_range_ts, during=Hour):
    over_ts = ts2HourlyTime(over_ts, MinInterval)
    interval = (over_ts - start_ts) / during
    
    emotion_dic = {}

    if during <= MinInterval:
        for k, v in emotions_kv.iteritems():
            weibos = read_weibo_results(v, over_ts=over_ts, during=during)
            emotion_dic[k] = weibos
    else:
        end_ts = over_ts
        start_ts = end_ts - during
        
        for k, v in emotions_kv.iteritems():
            weibos = read_weibo_results(v, start_ts=start_ts, over_ts=end_ts, during=during)
            emotion_dic[k] = weibos

    return emotion_dic


def read_range_kcount_results(start_ts=start_range_ts, over_ts=end_range_ts, during=Hour):
    over_ts = ts2HourlyTime(over_ts, MinInterval)
    interval = (over_ts - start_ts) / during
    
    emotion_dic = {}

    if during <= MinInterval:
        for k, v in emotions_kv.iteritems():
            kcount = read_kcount_results(v, over_ts=over_ts, during=during)
            emotion_dic[k] = kcount

    else:
        end_ts = over_ts
        start_ts = end_ts - during 
        
        for k, v in emotions_kv.iteritems():
            kcount = read_kcount_results(v, start_ts=start_ts, over_ts=end_ts, during=during)
            emotion_dic[k] = kcount

    print emotion_dic

    return emotion_dic


def read_range_count_results(start_ts=start_range_ts, over_ts=end_range_ts, during=Hour):
    over_ts = ts2HourlyTime(over_ts, MinInterval)
    interval = (over_ts - start_ts) / during
    
    emotion_dic = {}

    if during <= MinInterval:
        for k, v in emotions_kv.iteritems():
            count = read_count_results(v, over_ts=over_ts, during=during)
            emotion_dic[k] = [over_ts * 1000, count]
    else:
        end_ts = over_ts
        start_ts = end_ts - during 
        for k, v in emotions_kv.iteritems():
            count = read_count_results(v, start_ts=start_ts, over_ts=end_ts, during=during)
            emotion_dic[k] = [end_ts * 1000, count]

    print emotion_dic

    return emotion_dic


def getWeiboByMid(mid, emotion):
    weibo = xapian_search_weibo.search_by_id(int(mid), fields=['text', 'timestamp', 'user', 'retweeted_status', 'reposts_count'])
    if weibo:
        r = weibo
        uid = r['user']
        name = 'Unknown'
        text = r['text']
        reposts_count = r['reposts_count']
        retweeted_mid = r['retweeted_status']
        retweeted_text = 'None'
        if retweeted_mid:
            rweibo = xapian_search_weibo.search_by_id(int(retweeted_mid), fields=['text'])
            if rweibo:
                retweeted_text = rweibo['text']                
        ts = r['timestamp']
        date = datetime.date.fromtimestamp(int(ts)).isoformat()
        if getUsernameByUid(uid):
            name = getUsernameByUid(uid)
        user_link = "http://weibo.com/u/" + str(uid)
        weibo_link = weiboinfo2url(uid, mid)
        return [emotion, name, user_link, text, weibo_link, int(ts), reposts_count, retweeted_text]


def getUsernameByUid(uid):
    if not uid:
        return None
    user = xapian_search_user.search_by_id(int(uid), fields=['name'])
    if user:
        name = user['name']
        return name
    return None


def weiboinfo2url(uid, mid):
    return "http://weibo.com/{uid}/{mid}".format(uid=uid, mid=mid_to_str(mid))


def mid_to_str(mid):
    mid = str(mid)
    id1 = mid[0: 2]
    id2 = mid[2: 9]
    id3 = mid[9: 16]
    id_list = [id1, id2, id3]
    id_list = [base62_encode(int(mid)) for mid in id_list]
    return "".join(map(str, id_list))


def base62_encode(num, alphabet=ALPHABET):
    """Encode a number in Base X

    `num`: The number to encode
    `alphabet`: The alphabet to use for encoding
    """
    if (num == 0):
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        rem = num % base
        num = num // base
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)


def st_variation(lis1, lis2, lis3):
    ave = np.mean(lis1)    
    variation1 = [np.abs(num - ave)/ave for num in lis1]
    ave = np.mean(lis2)
    variation2 = [np.abs(num - ave)/ave for num in lis2]
    ave = np.mean(lis3)
    variation3 = [np.abs(num - ave)/ave for num in lis3]
    variation = [variation1[cursor]+variation2[cursor]+variation3[cursor] for cursor in range(len(lis1))]
    return variation


def find_topN(lis,n):
    new = [lis[0]]
    rank = [0]
    num_cursor = 1
    for num in lis[1:]:
        num_cursor += 1
        find = 0
        cursor = 0
        if num > new[0]:
            new[0:0] = [num]
            rank[0:0] = [num_cursor-1]
        else:
            for i in new:
                if num > i:
                    new[cursor:cursor] = [num]
                    rank[cursor:cursor] = [num_cursor-1]
                    find = 1
                    break
                cursor += 1
            if find == 0:
                new.append(num)
                rank.append(num_cursor-1)
            
    peak_x = []
    peak_y = []
    cursor = 0
    for y in new:
        if rank[cursor]!=0 and rank[cursor]!=len(new)-1:
            if y > lis[rank[cursor]+1] and y > lis[rank[cursor]-1]:
                peak_x.append(rank[cursor])
                peak_y.append(y)

        elif rank[cursor]==0:
            if y > lis[rank[cursor]+1]:
                peak_x.append(rank[cursor])
                peak_y.append(y)
        elif rank[cursor]==rank[cursor]!=len(new)-1:
            if y > lis[rank[cursor]+1]:
                peak_x.append(rank[cursor])
                peak_y.append(y)
        if len(peak_x)==n:
            break
        cursor += 1
    return peak_x[:n],peak_y[:n]


def save_rt_results(calc, query, results, during, klimit=TOP_KEYWORDS_LIMIT, wlimit=TOP_WEIBOS_LIMIT):
    if calc == 'count':
        for k, v in results.iteritems():
            sentiment = k
            ts, count = v
            item = SentimentTopicCount(query, during, ts, sentiment, count)
            item_exist = SentimentTopicCount(query=query, range=during, end=ts, sentiment=sentiment).first()
            if item_exist:
                db.session.delete(item_exist)
            db.session.add(item)
        
        db.session.commit()

    if calc == 'kcount':
        for k, v in results.iteritems():
            sentiment = k
            ts, kcount = v
            item = SentimentTopicKeywords(query, during, ts, sentiment, json.dumps(kcount))
            item_exist = SentimentTopicKeywords.query.filter_by(query=query, range=during, end=ts, limit=klimit, sentiment=sentiment).first()
            if item_exist:
                db.session.delete(item_exist)
            db.session.add(item)
        
        db.session.commit()

    if calc == 'weibos':
        for k, v in results.iteritems():
            sentiment = k
            ts, weibos = v
            item = SentimentTopicTopWeibos(query, during, ts, sentiment, json.dumps(weibos))
            item_exist = SentimentTopicTopWeibos.query.filter_by(query=query, range=during, end=ts, limit=wlimit, sentiment=sentiment).first()
            if item_exist:
                db.session.delete(item_exist)
            db.session.add(item)
        
        db.session.commit()


def sentimentRealTime(end_ts, during, method='whole', calc='count', query=None):
    emotions_count = {}
    emotions_kcount = {}
    emotions_weibo = {}

    if method == 'topic' and query and query != '':
        query_dict = {
            'timestamp': {'$gt': end_ts-during, '$lt': end_ts},
            '$or': []
        }

        for term in query.strip().split(','):
            if term:
                query_dict['$or'].append({'text': [term]})

        for k, v in emotions_kv.iteritems():
            query_dict['sentiment'] = v

            if calc == 'all':
                scount = xapian_search_weibo.search(query=query_dict, count_only=True)
                count, get_results = xapian_search_weibo.search(query=query_dict, fields=save_fields, \
                                                                sort_by=[sort_field], max_offset=w_limit)
                kcount = top_keywords(get_results, top=k_limit)
                top_ws = top_weibos(get_results, top=w_limit)
                emotions_count[v] = [end_ts, scount]
                emotions_kcount[v] = [end_ts, kcount]
                emotions_weibo[v] = [end_ts, top_ws]
                save_rt_results('count', query, emotions_count, during)
                save_rt_results('kcount', query, emotions_kcount, during, klimit=TOP_KEYWORDS_LIMIT)
                save_rt_results('weibos', query, emotions_weibo, during, wlimit=TOP_WEIBOS_LIMIT)
            
            elif calc == 'count':
                scount = xapian_search_weibo.search(query=query_dict, count_only=True)
                emotions_count[v] = [end_ts, scount]
                save_rt_results('count', query, emotions_count, during)
            
            else:
                count, get_results = xapian_search_weibo.search(query=query_dict, fields=save_fields, \
                                                                sort_by=[sort_field], max_offset=w_limit)
                if calc == 'kcount':
                    kcount = top_keywords(get_results, top=k_limit)
                    emotions_kcount[v] = [end_ts, kcount]
                    save_rt_results('kcount', query, emotions_kcount, during, TOP_KEYWORDS_LIMIT)

                if calc == 'weibos':
                    top_ws = top_weibos(get_results, top=w_limit)
                    emotions_weibo[v] = [end_ts, top_ws]
                    save_rt_results('weibos', query, emotions_weibo, during, TOP_WEIBOS_LIMIT)
        
        return {'count': emotions_count, 'kcount': emotions_kcount, 'weibos': emotions_weibo}


def sentimentFromDB(end_ts, during, method='whole', calc='count', query=None):
    emotions_count = {}
    emotions_kcount = {}
    emotions_weibo = {}

    if method == 'topic':
        for k, v in emotions_kv.iteritems():
            if calc == 'count':
                print query, end_ts, during, v
                tcount = SentimentTopicCount.query.filter_by(query=query, end=end_ts, range=during, sentiment=v).first()
                if tcount:
                    emoitions_count[k] = [end_ts*1000, tcount]
                else:
                    return None

            if calc == 'kcount':
                kcount = SentimentKeywordsCount.query.filter_by(query=query, end=end_ts, range=during, sentiment=v).first()
                if kcount:
                    emotions_kcount[k] = [end_ts*1000, kcount]
                else:
                    return None

            if calc == 'weibos':
                weibos = SentimentTopicTopWeibos.query.filter_by(query=query, end=end_ts, range=during, sentiment=v).first()
                if weibos:
                    emotions_weibo[k] = [end_ts*1000, weibos]
                else:
                    return None

    if method == 'whole':
        begin_ts = end_ts - during
        if calc == 'count':
            emotions_count = read_range_count_results(begin_ts, end_ts, during)
        
        if calc == 'kcount':
            emotions_kcount = read_range_kcount_results(begin_ts, end_ts, during)

        if calc == 'weibos':
            emotions_weibo = read_range_weibos_results(begin_ts, end_ts, during)

    if method == 'domain':
        pass

    return {'count': emotions_count, 'kcount': emotions_kcount, 'weibos': emotions_weibo}


def SentimentKeywordsRealTime(end_ts, during, method='whole', query=None):
    emotions_data = {}

    if method == 'topic' and query and query != '':
        query_dict = {
            'timestamp': {'$gt': end_ts-during, '$lt': end_ts},
            '$or': []
        }

        for term in query.strip().split(','):
            if term:
                query_dict['$or'].append({'text': [term]})

        for k, v in emotions_kv.iteritems():
            query_dict['sentiment'] = v
            count = xapian_search_weibo.search(query=query_dict, count_only=True)
            emotions_data[v] = [end_ts, kcount]
        
        for k, v in emotions_data.iteritems():
            end_ts, count = v
            item = SentimentTopicKeywords(query, during, end_ts, k, count) 
            item_exist = db.session.query(SentimentTopicKeywords).filter_by(query=query, end=end_ts, range=during, sentiment=k).first()
            if item_exist:
                db.session.delete(item_exist)
            db.session.add(item)
            
        db.session.commit()
        
        return emotions_data


if __name__ == '__main__':
    print os.getcwd()