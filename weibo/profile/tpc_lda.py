# -*- coding:utf-8 -*-

import gensim
from gensim import corpora, models, similarities
from xapian_weibo.xapian_backend import XapianSearch
import time
import datetime
from datetime import date
from operator import itemgetter
import json
import leveldb
import os

s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo')

###获取停用词表
def getStopWds(filename):
    swds = set()
    f = open(filename, 'r')
    count = 0
    for line in f.readlines():
        word = line.split()[0]
        swds.add(word)
        count += 1
    print 'stop words : ', count
    return swds

###获取当前日期字符串如“20130509”
def getNowDateStr():
    nowdate = date.today()
    return nowdate.isoformat()

def load_daily_user_tokens():
    LEVELDBPATH = '/home/mirage/leveldb'
    user_daily_tokens_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_tokens'),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    total_days = 89
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600
    begin_ts = now_ts - total_days * during

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': now_ts}
    }
    count, get_results = s.search(query=query_dict, fields=['user', 'terms', 'timestamp'])
    print count

    count = 0
    ts = te = time.time()
    for r in get_results():
        if 'user' in r and r['user']:
            uid = r['user']
        else:
            count += 1
            continue

        for i in xrange(-total_days + 1, 1):
            gt = now_ts + during * (i - 1)
            lt = now_ts + during * i
            if gt < r['timestamp'] <= lt:
                terms = r['terms']
                try:
                    k = str(uid) + '_' + str(lt)
                    v = user_daily_tokens_bucket.Get(k)
                    terms_dict = json.loads(v)
                    terms_dict.update(terms)
                    v = json.dumps(terms_dict)
                    user_daily_tokens_bucket.Put(k, v)
                except KeyError:
                    v = json.dumps(terms)
                    user_daily_tokens_bucket.Put(k, v)

        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
            print r['terms']
        count += 1

###生成并约简字典
def generate_dictionary_from_xapian(update_date=getNowDateStr()):
    '''使用5月14日往前推3个月（89天）的约2千万微博数据生成字典，并约简字典，字典规模约为100万左右，用时约24小时
    '''
    swds = getStopWds('./data/stpwds_linhao_20130514.txt')
    
    total_days = 89
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600
    begin_ts = now_ts - total_days * during

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': now_ts}
    }
    count, get_results = s.search(query=query_dict, fields=['terms', 'retweeted_status'])
    print count

    count = 0
    ts = te = time.time()
    docs = []
    for r in get_results():
        tokens = []
        for word, freq in r['terms'].items():
            if word not in swds:
                tokens.extend([word for i in xrange(0, freq)])
        docs.append(tokens)
        if 'retweeted_status' in r and r['retweeted_status']:
            retweeted_mid = r['retweeted_status']
            if retweeted_mid:
                r_count, r_results = s.search(query={'_id': int(retweeted_mid)}, fields=['terms'])
                if r_count:
                    for r_result in r_results():
                        r_tokens = []
                        for word, freq in r_result['terms'].items():
                            if word not in swds:
                                r_tokens.extend([word for i in xrange(0, freq)])
                        docs.append(r_tokens)
        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te

    ###生成并约简字典
    dictionary = corpora.Dictionary(docs)
    print 'Scale of dictionary before filter：%d' % len(dictionary.token2id)
    dictionary.filter_extremes(no_below=3, no_above=0.5, keep_n=1000000)
    dictionary.compactify()

    ###存储字典
    dictionary.save_as_text('./data/lda_%s_dict.txt' % update_date)
    print 'Scale of dictionary：%d' % len(dictionary.token2id)

def time2ts(date):
    import time
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def generate_dict_corpus_from_leveldb(update_date=getNowDateStr(), keep_number=200000):
    '''使用leveldb中的5月10日往前推3个月（89天）的约2千万微博数据生成字典，并约简字典，字典规模约为20万左右，用时约1小时
       使用leveldb中4月21日至5月21日的微博生成字典和语料，用时7分钟
       使用leveldb中5月1日至5月21日的微博生成字典和语料，用时5分钟
    '''
    LEVELDBPATH = '/home/mirage/leveldb'
    user_daily_tokens_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_tokens'),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    swds = getStopWds('./data/stpwds_linhao_20130514.txt')
    starttime = time2ts('2013-05-01')
    endtime = time2ts('2013-05-21')

    start_dict_time = time.time()
    print 'start dict generation at ', start_dict_time
    count = 0
    ts = te = time.time()
    docs = []
    results = user_daily_tokens_bucket.RangeIter()
    for k, v in results:
        uid, lt = k.split('_')
        tokens = []
        #每1万条0.5秒
        if int(lt) < endtime and int(lt) > starttime:
            for word, freq in json.loads(v).items():
                if word not in swds:
                    tokens.extend([word.encode('utf-8') for i in xrange(0, freq)])
            if len(tokens):
                docs.append(tokens)

        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
        count += 1

    ###生成并约简字典
    dictionary = corpora.Dictionary(docs)
    print 'Scale of dictionary before filter：%d' % len(dictionary.token2id)
    dictionary.filter_extremes(no_below=3, no_above=0.5, keep_n=keep_number)
    dictionary.compactify()

    ###存储字典
    dictionary.save_as_text('./data/lda_%s_dict.txt' % update_date)
    print 'Scale of dictionary after filter：%d' % len(dictionary.token2id)
    print 'finish dict generation and use time ', time.time() - start_dict_time

    ###生成语料库
    start_corpus_time = time.time()
    print 'start corpus generation at ', start_corpus_time
    corpus = [dictionary.doc2bow(doc) for doc in docs]
    corpora.MmCorpus.serialize('./data/lda_%s_corpus.mm' % update_date, corpus)
    print 'finish corpus output and use time ', time.time() - start_corpus_time

def generate_lda_corpus(update_date=getNowDateStr()):
    '''使用2013-05-16往前45天的约1000万微博数据生成corpus，用时约6个小时
    '''
    dictionary = corpora.Dictionary.load_from_text('./data/lda_2013-05-14_dict.txt')
    swds = getStopWds('./data/stpwds_linhao_20130514.txt')
    
    total_days = 60
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600
    begin_ts = now_ts - total_days * during

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': now_ts}
    }
    count, get_results = s.search(query=query_dict, fields=['terms', 'retweeted_status'])
    print count

    count = 0
    ts = te = time.time()
    corpus = []
    for r in get_results():
        tokens = []
        for word, freq in r['terms'].items():
            if word not in swds:
                tokens.extend([word for i in xrange(0, freq)])
        corpus.append(dictionary.doc2bow(tokens))
        if 'retweeted_status' in r and r['retweeted_status']:
            retweeted_mid = r['retweeted_status']
            if retweeted_mid:
                r_count, r_results = s.search(query={'_id': int(retweeted_mid)}, fields=['terms'])
                if r_count:
                    for r_result in r_results():
                        r_tokens = []
                        for word, freq in r_result['terms'].items():
                            if word not in swds:
                                r_tokens.extend([word for i in xrange(0, freq)])
                        corpus.append(dictionary.doc2bow(r_tokens))
        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te

    print 'finish corpus input'
    corpora.MmCorpus.serialize('./data/lda_%s_corpus.mm' % update_date, corpus)
    print 'finish corpus output'

def train_lda_model(update_date=getNowDateStr()):
    '''5-20训练用时约6小时
       5-21训练用时1小时
       5-22训练用时40分钟
    '''
    dictionary = corpora.Dictionary.load_from_text('./data/lda_2013-05-21_dict.txt')
    corpus = corpora.MmCorpus('./data/lda_2013-05-21_corpus.mm')

    ###生成tf-idf矩阵
    tfidf = models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]
    ###LDA模型训练
    print 'LDA training start:'
    start = time.time()
    lda = gensim.models.ldamodel.LdaModel(corpus=corpus_tfidf, id2word=dictionary, num_topics=100,
                                          update_every=1, chunksize=10000, passes=1)
    ###保存模型
    lda.save('./data/lda_%s_model.lda' % update_date)
    print 'training finish... use time ', time.time()-start
        
def load_daily_user_topics():
    ##提取已保存的模型
    start = time.time()
    dictionary = corpora.Dictionary.load_from_text('./data/lda_2013-05-21_dict.txt')
    lda_model = gensim.models.ldamodel.LdaModel.load('./data/lda_2013-05-22_model.lda')
    print 'load dict and model completed in ', time.time()-start, ' seconds'

    LEVELDBPATH = '/home/mirage/leveldb'
    user_daily_tokens_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_tokens'),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    user_daily_lda_topics_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_lda_topics'),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    
    count = 0
    ts = te = time.time()
    results = user_daily_tokens_bucket.RangeIter()
    for k, v in results:
        uid, lt = k.split('_')
        tokens = []
        #每1万条0.5秒
        for word, freq in json.loads(v).items():
            tokens.extend([word for i in xrange(0, freq)])
        #每1万条0.5秒
        doc_bow = dictionary.doc2bow(tokens)
        #每1万条5秒
        topic_prob = lda_model[doc_bow]
        
        topics = []
        '''
        if len(sorted(topic_prob, key=itemgetter(1), reverse=True)):
            topic, prob = sorted(topic_prob, key=itemgetter(1), reverse=True)[0]
        else:
            count += 1
            continue
        topics = lda_model.show_topic(topic)
        '''
        for topic, prob in sorted(topic_prob, key=itemgetter(1), reverse=True):
            topics.extend(lda_model.show_topic(topic))
        if len(topics):
            v = json.dumps(topics)
            user_daily_lda_topics_bucket.Put(k, v)
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
            print k, v
        count += 1
    print 'finish test model and use time: ', time.time()-start

if __name__ == '__main__':
    #load_daily_user_tokens()
    #generate_dict_corpus_from_leveldb()
    #train_lda_model('2013-05-22')
    load_daily_user_topics()