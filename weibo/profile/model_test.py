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
import logging

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
       使用leveldb中5月7日至5月10日的微博生成字典和语料，字典规模为6万，用时100s
    '''
    LEVELDBPATH = '/home/mirage/leveldb'
    user_daily_tokens_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_tokens'),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    swds = getStopWds('./data/stpwds_linhao_20130514.txt')
    starttime = time2ts('2013-05-07')
    endtime = time2ts('2013-05-10')

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
       5-25训练用时5分钟
    '''
    dictionary = corpora.Dictionary.load_from_text('./data/lda_2013-05-25_dict.txt')
    corpus = corpora.MmCorpus('./data/lda_2013-05-25_corpus.mm')

    ###生成tf-idf矩阵
    tfidf = models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]
    ###LDA模型训练
    print 'LDA training start:'
    start = time.time()
    lda = gensim.models.ldamodel.LdaModel(corpus=corpus_tfidf, id2word=dictionary, num_topics=100,
                                          update_every=0, chunksize=10000, passes=1)
    ###保存模型
    lda.save('./data/lda_%s_model.lda' % update_date)
    print 'training finish... use time ', time.time()-start
        
def load_daily_user_topics():
    ##提取已保存的模型
    start = time.time()
    dictionary = corpora.Dictionary.load_from_text('./data/lda_2013-05-25_dict.txt')
    lda_model = gensim.models.ldamodel.LdaModel.load('./data/lda_2013-05-26_model.lda')
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
        '''
        topic_prob = lda_model[doc_bow]
        
        topics = []
        
        for topic, prob in sorted(topic_prob, key=itemgetter(1), reverse=True):
            topics.extend(lda_model.show_topic(topic))
        if len(topics):
            v = json.dumps(topics)
            user_daily_lda_topics_bucket.Put(k, v)
        '''

        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
            print k, v
        count += 1
    print 'finish test model and use time: ', time.time()-start

def generate_test_corpus_for_sparselda():
    LEVELDBPATH = '/home/mirage/leveldb'
    user_daily_tokens_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_tokens'),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    swds = getStopWds('./data/stpwds_linhao_20130514.txt')
    starttime = time2ts('2013-05-07')
    endtime = time2ts('2013-05-10')

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

    f_corpus = open('/home/mirage/linhao/python-sparselda/testdata/corpus.txt', 'w')
    f_voc = open('/home/mirage/linhao/python-sparselda/testdata/vocabulary.txt', 'w')
    vocabulary = set()
    for tokens in docs:
        for word in tokens:
            f_corpus.write('%s\t' % word)
            vocabulary.add(word)
        f_corpus.write('\n')
    for token in vocabulary:
        f_voc.write('%s\n' % token)
    f_corpus.close()
    f_voc.close()

def getUserTopic(user_id):
    LEVELDBPATH = '/home/mirage/leveldb'
    import operator
    user_daily_lda_topics_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_lda_topics'),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    user_topics = {} 
    results = user_daily_lda_topics_bucket.RangeIter()
    count = 0
    ts = te = time.time()
    for k, v in results:
        uid, lt = k.split('_')
        if uid == str(user_id):
            topics = json.loads(v)
            for prob, topic in topics:
                try:
                    user_topics[topic] += prob
                except KeyError:
                    user_topics[topic] = prob

        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
        count += 1

    sorted_user_topics = sorted(user_topics.iteritems(), key=operator.itemgetter(1), reverse=True)

    f = open('./data/test/%s_topic.txt' % user_id, 'w')
    for topic, count in sorted_user_topics[:100]:
        try:
            f.write('%s %s\n' % (topic, count))
        except:
            f.write('%s %s\n' % (topic.encode('utf-8'), count))
        print topic, count
    f.close()

def getUserModel(user_id, update_date=getNowDateStr()):
    start_time = time.time()
    from xapian_weibo.utils import load_scws, cut
    s = load_scws()
    swds = getStopWds('./data/stpwds_linhao_20130514.txt')
    s_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo')
    
    begin_ts = time2ts('2009-01-01')
    now_ts = time2ts('2013-05-10')
    query_dict = {
        'user': user_id,
        'timestamp': {'$gt': begin_ts, '$lt': now_ts}
    }
    count, get_results = s_weibo.search(query=query_dict, fields=['text'])
    print count

    all_terms = []
    count = 0
    ts = te = time.time()
    for r in get_results():
        doc = []
        terms = cut(s, r['text'], f=['n'])
        for term in terms:
            if term not in swds:
                doc.append(term)
        all_terms.append(doc)
        if count % 1000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
        count += 1

    print 'read input data using time', time.time() - start_time 
    docs = all_terms
    ###生成并约简字典
    dictionary = corpora.Dictionary(docs)
    print 'Scale of dictionary before filter：%d' % len(dictionary.token2id)
    #dictionary.filter_extremes(no_below=3, no_above=0.5)
    #dictionary.compactify()
    print 'Scale of dictionary after filter：%d' % len(dictionary.token2id)
    dictionary.save_as_text('./data/test/%s_lda_%s_dict.txt' % (user_id, update_date))
    print 'finish dict generation and use time ', time.time() - start_time

    ###生成语料库
    corpus = [dictionary.doc2bow(doc) for doc in docs]
    corpora.MmCorpus.serialize('./data/test/%s_lda_%s_corpus.mm' % (user_id, update_date), corpus)
    corpus = corpora.MmCorpus('./data/test/%s_lda_%s_corpus.mm' % (user_id, update_date))
    print corpus
    print 'finish corpus output and use time ', time.time() - start_time


    ###生成tf-idf矩阵
    tfidf = models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]
    print 'finish corpus_tfidf and use time ', time.time() - start_time

    ###LDA模型训练
    print 'LDA training start:'
    lda = gensim.models.ldamodel.LdaModel(corpus=corpus_tfidf, id2word=dictionary, num_topics=100,
                                          update_every=1, chunksize=10000, passes=1)
    ###保存模型
    lda.save('./data/test/%s_lda_%s_model.lda' % (user_id, update_date))
    print 'training finish... use time ', time.time() - start_time

def getUserModelTopics(user_id, update_date=getNowDateStr(), style='documents'):
    lda_model = gensim.models.ldamodel.LdaModel.load('./data/test/%s_lda_%s_model.lda' % (user_id, update_date))
    start_time = time.time()
    topics = lda_model.show_topics(topics=-1, topn=10)
    print 'use time ', time.time()-start_time
    #topics = lda_model.print_topics()
    save_topics(topics, './data/test/%s_%s_%s_topics.txt' % (user_id, update_date, style))
def save_topics(topics, fname):
    f = open(fname, 'w')
    for topic in topics:
        f.write('%s\n' % topic)
    f.close()

def getLsiModel(user_id, update_date=getNowDateStr()):
    dictionary = corpora.Dictionary.load_from_text('./data/test/%s_lda_%s_dict.txt' % (user_id, update_date))
    corpus = corpora.MmCorpus('./data/test/%s_lda_%s_corpus.mm' % (user_id, update_date))
    tfidf = models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]
    start_time = time.time()
    lsi = gensim.models.lsimodel.LsiModel(corpus=corpus_tfidf, num_topics=100, id2word=dictionary)
    lsi.save('./data/test/%s_lsi_%s_model.lda' % (user_id, update_date))
    print 'model train using time ', time.time() - start_time

    topics = lsi.show_topics(num_topics=100, num_words=10)
    save_topics(topics, './data/test/%s_%s_lsi_topics.txt' % (user_id, update_date))
    print 'showing topics using time ', time.time() - start_time

def loadUserTopicWords(user_id, model, update_date=getNowDateStr()):
    path = './data/test/'
    f = open(path + '%s_%s_%s_topics.txt' % (user_id, update_date, model))
    topic_list = []
    for line in f.readlines():
        topics = line.strip().split('+')
        topic_line = {}
        for topic in topics:
            prob, word = topic.strip().split('*')
            prob = float(prob)
            if model == 'lsi':
                word = word.split("\"")[1]
            topic_line[word] = prob
        topic_list.append(topic_line)
    return topic_list    

def getInterestWords(user_id, texts):
    
    from xapian_weibo.utils import load_scws, cut
    s = load_scws()
    terms = cut(s, texts, f=['n'])
    f = open('./data/test/%s_interests.txt' % user_id, 'w')
    for term in set(terms):
        f.write('%s\n' % term)
    f.close()
    print len(set(terms))
    return set(terms)
    #return texts

def evaluate_model_precision(user_id, texts, update_date=getNowDateStr(), model='documents'):
    interests = getInterestWords(user_id, texts)
    words = loadUserTopicWords(user_id, model, update_date)
    hit_words = set()
    for doc in words:
        for word, prob in doc.iteritems():
            if word in interests:
                hit_words.add(word)
    if len(interests):
        precision = len(hit_words) * 1.0 / len(interests)
        print 'precision: ', precision
    else:
        print 'cant calculate because len(interests)=0'

def readUidByArea(area):
    uidlist = []
    with open("./data/uidlist_20130316/" + area + ".txt") as f:
        for line in f:
            uid = int(line.split()[0])
            uidlist.append(uid)
    return uidlist

classes = ["culture", "entertainment", "fashion",'education', "finance", "sports", "technology",'media']

def getFieldUser():    
    field_user = {}
    for field in classes:
        uids = readUidByArea(field)
        field_user[field] = uids
    return field_user

def getFieldModel(area, method='user_level'):
    print '%s %s model start...' % (area, method)
    start_time = time.time()
    field_user = getFieldUser()
    uids = field_user[area]
    from xapian_weibo.utils import load_scws, cut
    s_cut = load_scws()

    load_utils_time = time.time()
    print 'load utils...', load_utils_time - start_time

    docs = []
    
    if method == 'user_level':
        for uid in uids:
            count, get_results = s.search(query={'user': int(uid)}, fields=['text'])
            if count == 0:
                continue
            terms = []
            for r in get_results():
                terms.extend(cut(s_cut, r['text'], f=['n']))
            docs.append(terms)
    else:
        for uid in uids:
            count, get_results = s.search(query={'user': int(uid)}, fields=['text'])
            if count == 0:
                continue
            for r in get_results():
                docs.append(cut(s_cut, r['text'], f=['n']))

    load_docs_time = time.time()
    print 'load docs...', load_docs_time - load_utils_time

    ###生成并约简字典
    dictionary = corpora.Dictionary(docs)
    print 'Scale of dictionary before filter：%d' % len(dictionary.token2id)
    dictionary.filter_extremes(no_below=3, no_above=0.5)
    dictionary.compactify()
    print 'Scale of dictionary after filter：%d' % len(dictionary.token2id)
    dictionary.save_as_text('./data/test/%s_lda_%s_dict.txt' % (area, method))
    fin_dict_time = time.time()
    print 'finish dict generation...', fin_dict_time - load_docs_time

    ###生成语料库
    corpus = [dictionary.doc2bow(doc) for doc in docs]
    corpora.MmCorpus.serialize('./data/test/%s_lda_%s_corpus.mm' % (area, method), corpus)
    corpus = corpora.MmCorpus('./data/test/%s_lda_%s_corpus.mm' % (area, method))
    print corpus
    Mmcorpus_time = time.time()
    print 'finish Mmcorpus output... ', Mmcorpus_time - fin_dict_time

    ###生成tf-idf矩阵
    tfidf = models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]
    tf_time = time.time()
    print 'finish corpus_tfidf...', tf_time - Mmcorpus_time

    ###LDA模型训练
    print 'LDA training start:'
    lda = gensim.models.ldamodel.LdaModel(corpus=corpus_tfidf, id2word=dictionary, num_topics=100,
                                          update_every=1, chunksize=10000, passes=1)
    lda_model_time = time.time()
    print 'Lda training finish ...', lda_model_time - tf_time

    ###保存模型
    lda.save('./data/test/%s_lda_%s_model.lda' % (area, method))
    print 'training model saved and total use time...', time.time() - start_time

    ###保存话题关键词
    lda_model = gensim.models.ldamodel.LdaModel.load('./data/test/%s_lda_%s_model.lda' % (area, method))
    topics = lda_model.show_topics(topics=-1, topn=10)
    #topics = lda_model.print_topics()
    save_topics(topics, './data/test/%s_%s_topics.txt' % (area, method))
    print '%s %s topic words saved and total use time...' % (area, method), time.time() - start_time

def getUserTopics(user_id, update_date=getNowDateStr()):
    logger = logging.getLogger('user topic modeling')
    hdlr = logging.FileHandler('./data/test/user_topic_log/%s_%s.log' % (user_id, update_date))
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.INFO)

    print '%s %s topic model start...' % (user_id, update_date)
    logger.info('%s %s topic model start...' % (user_id, update_date))
    start_time = time.time()

    from xapian_weibo.utils import load_scws, cut
    s = load_scws()
    swds = getStopWds('./data/stpwds_linhao_20130514.txt')
    s_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo')

    load_utils_time = time.time()
    print 'load utils...', load_utils_time - start_time
    logger.info('load utils...%s' % (load_utils_time - start_time))

    count, get_results = s_weibo.search(query={'user': int(user_id)}, fields=['text'])
    print 'user words count', count
    logger.info('user words count...%s' % count)
    if count > 0:
        try:

            all_terms = []

            for r in get_results():
                doc = []
                terms = cut(s, r['text'], f=['n'])
                for term in terms:
                    if term not in swds:
                        doc.append(term)
                all_terms.append(doc)

            load_docs_time = time.time()
            print 'load docs...', load_docs_time - load_utils_time 
            logger.info('load docs...%s' % (load_docs_time - load_utils_time))
            docs = all_terms

            ###生成并约简字典
            dictionary = corpora.Dictionary(docs)
            print 'Scale of dictionary before filter：%d' % len(dictionary.token2id)
            logger.info('Scale of dictionary before filter：%d' % len(dictionary.token2id))
            '''
            dictionary.filter_extremes(no_below=3, no_above=0.5)
            dictionary.compactify()
            '''
            print 'Scale of dictionary after filter：%d' % len(dictionary.token2id)
            logger.info('Scale of dictionary after filter：%d' % len(dictionary.token2id))
            dictionary.save_as_text('./data/test/%s_lda_%s_dict.txt' % (user_id, update_date))
            fin_dict_time = time.time()
            print 'finish dict generation...', fin_dict_time - load_docs_time
            logger.info('finish dict generation...%s' % (fin_dict_time - load_docs_time))

            ###生成语料库
            corpus = [dictionary.doc2bow(doc) for doc in docs]
            corpora.MmCorpus.serialize('./data/test/%s_lda_%s_corpus.mm' % (user_id, update_date), corpus)
            corpus = corpora.MmCorpus('./data/test/%s_lda_%s_corpus.mm' % (user_id, update_date))
            print corpus
            Mmcorpus_time = time.time()
            print 'finish Mmcorpus output... ', Mmcorpus_time - fin_dict_time
            logger.info('finish Mmcorpus output...%s' % (Mmcorpus_time - fin_dict_time))

            ###生成tf-idf矩阵
            tfidf = models.TfidfModel(corpus)
            corpus_tfidf = tfidf[corpus]
            tf_time = time.time()
            print 'finish corpus_tfidf...', tf_time - Mmcorpus_time

            ###LDA模型训练
            print 'LDA training start:'
            lda = gensim.models.ldamodel.LdaModel(corpus=corpus_tfidf, id2word=dictionary, num_topics=100,
                                                  update_every=1, chunksize=10000, passes=1)
            lda_model_time = time.time()
            print 'Lda training finish ...', lda_model_time - tf_time
            logger.info('Lda training finish ...%s' % (lda_model_time - tf_time))

            ###保存模型
            lda.save('./data/test/%s_lda_%s_model.lda' % (user_id, update_date))
            print 'training model saved and total use time...', time.time() - start_time
            logger.info('training model saved and total use time...%s' % (time.time() - start_time))

            ###保存话题关键词
            lda_model = gensim.models.ldamodel.LdaModel.load('./data/test/%s_lda_%s_model.lda' % (user_id, update_date))
            topics = lda_model.show_topics(topics=-1, topn=10)
            save_topics(topics, './data/test/%s_%s_topics.txt' % (user_id, update_date))
            print '%s %s topic words saved and total use time...' % (user_id, update_date), time.time() - start_time
            logger.info('%s %s topic words saved and total use time...%s' % (user_id, update_date, (time.time() - start_time)))
        except ValueError:
            pass

def readUserTopics2Leveldb():
    LEVELDBPATH = '/home/mirage/leveldb'
    user_daily_topics_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_tokens'),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))


def test_vsm():
    corpus = [[(0, 1), (1, 1), (2, 1)],[(0, 1), (3, 1), (4, 1), (5, 1), (6, 1), (7, 1)],[(2, 1), (5, 1), (7, 1), (8, 1)],
              [(1, 1), (5, 2), (8, 1)],[(3, 1), (6, 1), (7, 1)],[(9, 1)],[(9, 1), (10, 1)],[(9, 1), (10, 1), (11, 1)],
              [(4, 1), (10, 1), (11, 1)]]
    corpora.MmCorpus.serialize('./data/test_corpus.mm', corpus)

if __name__ == '__main__':
    #load_daily_user_tokens()
    #generate_dict_corpus_from_leveldb(keep_number=1000)
    #train_lda_model()
    #load_daily_user_topics()
    #generate_test_corpus_for_sparselda()
    #getUserTopic(1813080181))
    #getUserModel(1813080181)
    #getUserModelTopics(1813080181, '2013-05-29')
    #getLsiModel(1813080181, '2013-05-29')
    #getUserModel(1182391231)
    #getUserModelTopics(1182391231)
    #getLsiModel(1182391231)

    #getUserModel(1182389073)
    #getUserModel(1187986757)
    #getUserModel(1193491727)
    '''
    getUserModel(1195818302)
    getUserModel(1222713954)
    getUserModel(1495033135)
    getUserModel(1523766213)
    getUserModel(1255849511)
    

    getLsiModel(1182389073)
    getLsiModel(1187986757)
    getLsiModel(1193491727)
    getLsiModel(1195818302)
    getLsiModel(1222713954)
    getLsiModel(1495033135)
    getLsiModel(1523766213)
    getLsiModel(1255849511)
    
    getUserModelTopics(1182389073)
    getUserModelTopics(1187986757)
    getUserModelTopics(1193491727)
    getUserModelTopics(1195818302)
    getUserModelTopics(1222713954)
    getUserModelTopics(1495033135)
    getUserModelTopics(1523766213)
    getUserModelTopics(1255849511)
    '''
    '''
    evaluate_model_precision(1813080181, '天使投资人,微博打拐,乐观,癌症斗士,地震救助,UTStarCOM,8848,创业,移动互联网,天使投资', '2013-05-29', 'documents')
    evaluate_model_precision(1182391231, '纯洁,仁慈,意气风发,SOHO中国,董事长,年会,考验,困难,房产,杂谈,复星,起诉,诚实,不可逾越,底线,建筑质量,杂谈', '2013-05-30', 'documents')
    evaluate_model_precision(1182389073, '根据财政部发布数据，2010年全国财政收入83080亿元，其中：中央财政收入42470.52亿元，地方本级收入40609.8亿元。国有土地使用权出让收入29109.94亿元\
        。如果不考虑保障群体已有住宅面积，按照不同保障水平，根据2010年财政收入水平，一年需要的直接和间接补贴资金占地方财政本级收入的3.51%，4.15%和4.79%；占土地使用权出让收入的4.90%，\
        5.78%和6.68%[1]。而目前每年需要上万亿左右的保障房建设资金主要通过财政投入，这笔资金占地方财政收入的四分之一以上，给地方财政造成了很大的压力。导致的问题一是很多保障房项目由于\
        缺少资金不能按时推进；二是一些地方为了缓解资金压力，一次性收取多年租金，或者开始出售保障房换取资金回流。这些做法不仅降低了政府的声誉，也扭曲了保障房的性质，降低了保障的功能和\
        效率。随着城市化的发展，城市人口不断增长。中国发展研究基金会发布的《中国发展报告2010：促进人的发展的中国新型城市化战略》报告，预计在2030年我国的城市化率应该能够达到65%的水平，\
        其中从2010年到2020年每年增加城市化率一个百分点，从2020年到2030年每年增加城市化率0.8个百分点。我们设定2010年到2020年，城市中户籍与非户籍常住人口都以平均1%的速度增加，2020到2030年，\
        户籍和非户籍常住人口年增长0.8%，每年城镇人口变动情况如下。'\
        , '2013-05-30', 'documents')
    evaluate_model_precision(1187986757, '真格基金创始人,新东方联合创始人,创业,大学生 互联网,财经,文学,旅游,美,电影,音乐', '2013-05-30', 'documents')
    evaluate_model_precision(1193491727, '万科企业股份有限公司董事会主席,品牌,中国形象,纽约,时代广场,哈佛,MIT, 汪建,美国,蒙卡达,密集型产业,广州东方宾馆, 健康,洪海,珠峰,王静,老王', '2013-05-30', 'documents')
    evaluate_model_precision(1195818302, '常言道，梦醒是实现梦想的开始……请收看【视频：《大天下 晓视界》赵晓谈中国梦第3集——照进现实】 http://t.cn/zYDd9SM\
        北京科技大学管理学院教授,不唯上，不唯书，不唯众，不唯我 只唯实', '2013-05-30', 'documents')
    evaluate_model_precision(1222713954, '耶鲁大学管理学院金融经济学教授、清华大学人文社会科学院长江讲席教授。耶鲁大学金融学院教授  24堂，财富课，金融的逻辑，教授', '2013-05-30', 'documents')
    evaluate_model_precision(1495033135,'国务院发展研究中心金融所研究员、商务部经贸政策咨询委员会委员，博士生导师，中国银行业协会首席经济学家、中国证监会并购重组专家咨询委员会委员,国务院发展研究中心\
        金融所研究员、中国银行业协会首席经济学家,金融学博导，美食爱好者，财经阅读者，与读者互动平台，电影爱好者，金融政策研究者', '2013-05-30', 'documents')
    evaluate_model_precision(1523766213, '这里是成都市人民政府新闻办公室微博，我们将及时给朋友们提供有关成都的各类信息。 _____________________ 成都市市...,成都街巷志，现在发布，公益成都，天府之国成都\
        ，熊猫故里，政务公开，新闻传播，成都美食，成都旅游，成都求职', '2013-05-30', 'documents')
    evaluate_model_precision(1255849511, '大智兴邦, 不过集众思 电视台主持人  财经，主持人  虚实之间，名人，明星', '2013-05-30', 'documents')
    '''
    '''
    evaluate_model_precision(1813080181, '天使投资人,微博打拐,乐观,癌症斗士,地震救助,UTStarCOM,8848,创业,移动互联网,天使投资', '2013-05-29', 'lsi')
    evaluate_model_precision(1182391231, '纯洁,仁慈,意气风发,SOHO中国,董事长,年会,考验,困难,房产,杂谈,复星,起诉,诚实,不可逾越,底线,建筑质量,杂谈', '2013-05-30', 'lsi')
    evaluate_model_precision(1182389073, '根据财政部发布数据，2010年全国财政收入83080亿元，其中：中央财政收入42470.52亿元，地方本级收入40609.8亿元。国有土地使用权出让收入29109.94亿元\
        。如果不考虑保障群体已有住宅面积，按照不同保障水平，根据2010年财政收入水平，一年需要的直接和间接补贴资金占地方财政本级收入的3.51%，4.15%和4.79%；占土地使用权出让收入的4.90%，\
        5.78%和6.68%[1]。而目前每年需要上万亿左右的保障房建设资金主要通过财政投入，这笔资金占地方财政收入的四分之一以上，给地方财政造成了很大的压力。导致的问题一是很多保障房项目由于\
        缺少资金不能按时推进；二是一些地方为了缓解资金压力，一次性收取多年租金，或者开始出售保障房换取资金回流。这些做法不仅降低了政府的声誉，也扭曲了保障房的性质，降低了保障的功能和\
        效率。随着城市化的发展，城市人口不断增长。中国发展研究基金会发布的《中国发展报告2010：促进人的发展的中国新型城市化战略》报告，预计在2030年我国的城市化率应该能够达到65%的水平，\
        其中从2010年到2020年每年增加城市化率一个百分点，从2020年到2030年每年增加城市化率0.8个百分点。我们设定2010年到2020年，城市中户籍与非户籍常住人口都以平均1%的速度增加，2020到2030年，\
        户籍和非户籍常住人口年增长0.8%，每年城镇人口变动情况如下。'\
        , '2013-05-30', 'lsi')
    evaluate_model_precision(1187986757, '真格基金创始人,新东方联合创始人,创业,大学生 互联网,财经,文学,旅游,美,电影,音乐', '2013-05-30', 'lsi')
    evaluate_model_precision(1193491727, '万科企业股份有限公司董事会主席,品牌,中国形象,纽约,时代广场,哈佛,MIT, 汪建,美国,蒙卡达,密集型产业,广州东方宾馆, 健康,洪海,珠峰,王静,老王', '2013-05-30', 'documents')
    evaluate_model_precision(1195818302, '常言道，梦醒是实现梦想的开始……请收看【视频：《大天下 晓视界》赵晓谈中国梦第3集——照进现实】 http://t.cn/zYDd9SM\
        北京科技大学管理学院教授,不唯上，不唯书，不唯众，不唯我 只唯实', '2013-05-30', 'lsi')
    evaluate_model_precision(1222713954, '耶鲁大学管理学院金融经济学教授、清华大学人文社会科学院长江讲席教授。耶鲁大学金融学院教授  24堂，财富课，金融的逻辑，教授', '2013-05-30', 'lsi')
    evaluate_model_precision(1495033135,'国务院发展研究中心金融所研究员、商务部经贸政策咨询委员会委员，博士生导师，中国银行业协会首席经济学家、中国证监会并购重组专家咨询委员会委员,国务院发展研究中心\
        金融所研究员、中国银行业协会首席经济学家,金融学博导，美食爱好者，财经阅读者，与读者互动平台，电影爱好者，金融政策研究者', '2013-05-30', 'lsi')
    evaluate_model_precision(1523766213, '这里是成都市人民政府新闻办公室微博，我们将及时给朋友们提供有关成都的各类信息。 _____________________ 成都市市...,成都街巷志，现在发布，公益成都，天府之国成都\
        ，熊猫故里，政务公开，新闻传播，成都美食，成都旅游，成都求职', '2013-05-30', 'lsi')
    evaluate_model_precision(1255849511, '大智兴邦, 不过集众思 电视台主持人  财经，主持人  虚实之间，名人，明星', '2013-05-30', 'lsi')
    '''
    #getFieldModel('finance')
    #getFieldModel('finance', 'weibo_level')
    '''
    getFieldModel('culture')
    getFieldModel('culture', 'weibo_level')
    
    getFieldModel('entertainment')
    getFieldModel('entertainment', 'weibo_level')
    
    getFieldModel('fashion')
    getFieldModel('fashion', 'weibo_level')
    
    getFieldModel('education')
    getFieldModel('education', 'weibo_level')
    
    getFieldModel('sports')
    getFieldModel('sports', 'weibo_level')
    
    getFieldModel('technology')
    getFieldModel('technology', 'weibo_level')

    getFieldModel('media')
    getFieldModel('media', 'weibo_level')
    '''
    '''
    all_time = time.time()
    classes = ["culture", "entertainment", "fashion",'education', "finance", "sports", "technology",'media']
    field_user = getFieldUser()
    for area in classes:
        uids = field_user[area]
        for uid in uids:
            getUserTopics(uid)
    print 'total use time', time.time() - all_time
    '''
    test_vsm()







