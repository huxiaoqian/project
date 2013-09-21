# -*- coding: utf-8 -*-
import time
import json
from view import app

app = app.test_client()

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r args: %s %2.2f sec' % (method.__name__, args, te - ts)
        return result
    return timed


def date2ts(datestr):
    return int(time.mktime(time.strptime(datestr, '%Y-%m-%d  %H:%M:%S')))

@timeit
def emotion_data_simulation_test(timestamp, query_str=None):
    if query_str:
        rv = app.get('/moodlens/data/global/?query=' + query_str + '&ts=' + str(timestamp))
    else:
        rv = app.get('/moodlens/data/global/?ts=' + str(timestamp))
    rv_data = json.loads(rv.data)
    print rv_data['happy'][1], rv_data['angry'][1], rv_data['sad'][1]
    return rv_data['happy'][1] + rv_data['angry'][1] + rv_data['sad'][1]

@timeit
def words10testemotion():
    query_str_list = ['春节', '两会', '中国', '日本', '新年', '星座', '情人节', '礼物', '除夕', '三十']
    total_count = 0
    timestamp = date2ts('2013-02-09 02:00:00')
    for i in range(1, 11):
        total_count += emotion_data_simulation_test(timestamp, query_str_list[i-1])
    print total_count

@timeit
def days30testemotion(query_str):
    total_count = 0
    endts = date2ts('2013-03-01 02:00:00')
    for i in range(0, 30):
        print i
        timestamp = endts - i * 24 * 3600
        total_count += emotion_data_simulation_test(timestamp, query_str)
    print 'total_count: ', total_count

@timeit
def emotion_keywords_data_simulation_test(timestamp, query_str=None, emotion=None):
    if query_str:
        if emotion:
            rv = app.get('/moodlens/keywords_data/global/?query=' + query_str + '&ts=' + str(timestamp) + '&emotion=' + emotion)
        else:
            rv = app.get('/moodlens/keywords_data/global/?query=' + query_str + '&ts=' + str(timestamp))   
    else:
        if emotion:
            rv = app.get('/moodlens/keywords_data/global/?ts=' + str(timestamp) + '&emotion=' + emotion)
        else:
            rv = app.get('/moodlens/keywords_data/global/?ts=' + str(timestamp))
    rv_data = json.loads(rv.data)
    return len(rv_data)

@timeit
def words10testkeywords(emotion):
    query_str_list = ['春节', '两会', '中国', '日本', '新年', '星座', '情人节', '礼物', '除夕', '三十']
    total_count = 0
    timestamp = date2ts('2013-02-09 02:00:00')
    for i in range(1, 11):
        total_count += emotion_keywords_data_simulation_test(timestamp, query_str_list[i-1], emotion)
    print total_count

@timeit
def days30testkeywords(query_str, emotion):
    total_count = 0
    endts = date2ts('2013-03-01 02:00:00')
    for i in range(0, 30):
        print i
        timestamp = endts - i * 24 * 3600
        total_count += emotion_keywords_data_simulation_test(timestamp, query_str, emotion)
    print 'total_count: ', total_count

@timeit
def emotion_weibos_data_simulation_test(timestamp, query_str=None, emotion=None):
    if emotion:
        if query_str:
            rv = app.get('/moodlens/weibos_data/' + emotion + '/global/?query=' + query_str + '&ts=' + str(timestamp))  
        else:
            rv = app.get('/moodlens/weibos_data/' + emotion + '/global/?ts=' + str(timestamp))
        rv_data = json.loads(rv.data)
        return len(rv_data)

@timeit
def words10testweibos(emotion):
    query_str_list = ['春节', '两会', '中国', '日本', '新年', '星座', '情人节', '礼物', '除夕', '三十']
    total_count = 0
    timestamp = date2ts('2013-02-09 02:00:00')
    for i in range(1, 11):
        total_count += emotion_weibos_data_simulation_test(timestamp, query_str_list[i-1], emotion)
    print total_count

@timeit
def days30testweibos(query_str, emotion):
    total_count = 0
    endts = date2ts('2013-03-01 02:00:00')
    for i in range(0, 30):
        print i
        timestamp = endts - i * 24 * 3600
        total_count += emotion_weibos_data_simulation_test(timestamp, query_str, emotion)
    print 'total_count: ', total_count

@timeit
def emotion_peaks_data_simulation_test(query_str=None, total_days=30):
    happy_list, angry_list, sad_list, ts_list = emotion_peaks_prepare(query_str, total_days)
    emotion_peaks_calc(happy_list, angry_list, sad_list, ts_list, query_str)

@timeit
def emotion_peaks_prepare(query_str=None, total_days=30):
    during = 24 * 3600
    endts = date2ts('2013-03-01 02:00:00')
    happy_list = []
    angry_list = []
    sad_list = []
    ts_list = []
    for i in range(0, total_days):
        endts = endts - during
        if query_str:
            rv = app.get('/moodlens/data/global/?query=' + query_str  + '&ts=' + str(endts))
        else:
            rv = app.get('/moodlens/data/global/?ts=' + str(endts))
        rv_data = json.loads(rv.data)
        if rv_data['happy'][1] + rv_data['angry'][1] + rv_data['sad'][1] > 0:
            t_count = rv_data['happy'][1] + rv_data['angry'][1] + rv_data['sad'][1]
            happy_list.append(str(rv_data['happy'][1]*1.0 / t_count))
            angry_list.append(str(rv_data['angry'][1]*1.0 / t_count))
            sad_list.append(str(rv_data['sad'][1]*1.0 / t_count))
            ts_list.append(str(rv_data['happy'][0] / 1000))
    return happy_list, angry_list, sad_list, ts_list

@timeit
def emotion_peaks_calc(happy_list, angry_list, sad_list, ts_list, query_str=None):
    if query_str:
        peak_url = '/moodlens/emotionpeak/?happy=' + ','.join(happy_list) + '&angry=' + ','.join(angry_list) + '&sad=' + ','.join(sad_list) + '&ts=' + ','.join(ts_list) + '&query=' + query_str
    else:
        peak_url = '/moodlens/emotionpeak/?happy=' + ','.join(happy_list) + '&angry=' + ','.join(angry_list) + '&sad=' + ','.join(sad_list) + '&ts=' + ','.join(ts_list)
    rv = app.get(peak_url)
    rv_data = json.loads(rv.data)
    print '#peaks: ', len(rv_data)


def main():
    '''情感分类数据检索测试
    '''
    # #hit(1 day, count only test, angry+happy+sad): 55089
    # #time: 3.92s, avg_time_per_word_per_day: 0.39s, avg_time_per_10000hits: 0.71s
    # words10testemotion()

    # #hit(30 days, count only test, angry+happy+sad): 207653
    # #time: 15.04s, avg_time_per_10000hits: 0.72s, avg_time_per_day: 0.50s
    # days30testemotion('春节')

    # #hit(30 days, count only test, angry+happy+sad): 7104734
    # #time: 186.32s, avg_time_per_10000hits: 0.26s, avg_time_per_day: 6.211s
    # days30testemotion(None)

    '''情感关键词检索测试
    '''
    # #keywords count(1 day, keywords_with_count(50 top frequent words), angry+happy+sad): 500
    # #time: 20.76s, avg_time_per_50count_per_day: 2.076s
    # words10testkeywords(None)

    # #keywords count(30 days, keywords_with_count(50 top frequent words), angry+happy+sad): 50*30=1500
    # #time: 85.39s, avg_time_per_50count_per_day: 2.85s
    # days30testkeywords('春节', None)

    # #keywords count(30 days, keywords_with_count(50 top frequent words), angry+happy+sad): 50*30=1500
    # #time: 845.05s, avg_time_per_50count_per_day: 28.17s
    # days30testkeywords(None, None)

    # #keywords count(1 day, keywords_with_count(50 top frequent words), happy): 500
    # #time: 19.44s, avg_time_per_50count_per_day: 1.944s
    # words10testkeywords('happy')

    # #keywords count(30 days, keywords_with_count(50 top frequent words), happy): 50*30=1500
    # #time: 65.73s, avg_time_per_50count_per_day: 2.19s
    # days30testkeywords('春节', 'happy')

    # #keywords count(30 days, keywords_with_count(50 top frequent words), happy): 50*30=1500
    # #time: 837.34s, avg_time_per_50count_per_day: 27.91s
    # days30testkeywords(None, 'happy')

    '''情感关键微博检索测试
    '''
    # #keyweibos count(1 day, weibos_with_info(10 top weibos sorted by reposts count), happy): 100
    # #time: 6.29s, avg_time_per_10count_per_day: 0.629s
    # words10testweibos('happy')

    # #keyweibos count(30 days, weibos_with_info(10 top weibos sorted by reposts count), happy): 300
    # #time: 8.35s, avg_time_per_10count_per_day: 0.278s 
    # days30testweibos('春节', 'happy')

    # #keyweibos count(30 days, weibos_with_info(10 top weibos sorted by reposts count), happy): 300
    # #time: 121.46s, avg_time_per_10count_per_day: 4.05s 
    # days30testweibos(None, 'happy')

    '''情绪拐点测试
    '''
    # #peaks_with_ts&title&text, 30 days, 10 peaks
    # #time: 51.86s
    emotion_peaks_data_simulation_test('春节', 30)
    
    # #peaks_with_ts&title&text, 20 days, 7 peaks
    # #time: 32.44s
    # emotion_peaks_data_simulation_test('春节', 20)

    # #peaks_with_ts&title&text, 10 days, 3 peaks
    # #time: 9.53s
    # emotion_peaks_data_simulation_test('春节', 10)

    # #peaks_with_ts&title&text, 30 days, 8 peaks
    # #time: 765.80s
    # emotion_peaks_data_simulation_test(None, 30)
    
    # #peaks_with_ts&title&text, 20 days, 7 peaks
    # #time: 645.29s
    # emotion_peaks_data_simulation_test(None, 20)
    
    # #peaks_with_ts&title&text, 10 days, 3 peaks
    # #time: 257.64s
    # emotion_peaks_data_simulation_test(None, 10)

    '''
    性能测试结论
    1.从搜索引擎根据关键字检索微博的效率不够稳定，对于未曾检索过的冷数据，查询很慢，对于已经检索过的数据，由于缓存机制查询速度会大幅度提升。
    2.第一次检索用时往往是第二次检索用时的2倍

    '''


if __name__ == '__main__':
    main()