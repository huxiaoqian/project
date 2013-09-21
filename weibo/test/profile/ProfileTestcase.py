# -*- coding: utf-8 -*-
"""
    Tests in Profile Module
    Tests the flask application
"""

import unittest
from view import app
import time
import json


class ProfileTestCase(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        print 'Profile Module test set up'
        self.app = app.test_client()

    def tearDown(self):
        print 'Profile Module test tear down'

    def test_person_weibo_count(self):
        rv = self.app.get('/profile/person_count/1813080181?interval=300')
        rv_data = json.loads(rv.data)
        rv_test = self.app.get('/profile/person_count_false/1813080181?interval=300')
        rv_data_test = json.loads(rv_test.data)
        #self.assertTrue(rv_data==rv_data_test, 'data is not equal')
        # self.assertEqual(rv_data, rv_data_test)
        print rv_data
        print rv_data_test
    
    '''
    def test_emotion_data(self):
        querystr_split_by_english_comma = '春节,两会'
        querystr_split_by_chinese_comma = '春节，两会'
        querystr_chunjie = ''
        timestamp = date2ts('2013-02-09 02:00:00')

        self.assertIsNotNone(timestamp, 'timestamp should not be None')
        self.assertIsNotNone(querystr_split_by_english_comma, 'query string should not be None')

        rv = self.app.get('/moodlens/data/global/?query=' + querystr_split_by_chinese_comma  + '&ts=' + str(timestamp))
        rv_data = json.loads(rv.data)
        self.assertTrue('angry' in rv_data and 'happy' in rv_data and 'sad' in rv_data, 'emotion data result should have 3 emotion keys')
        self.assertEqual(rv_data['angry'][1], 0)
        self.assertEqual(rv_data['happy'][1], 0)
        self.assertEqual(rv_data['sad'][1], 0)
        
        rv = self.app.get('/moodlens/data/global/?query=' + querystr_split_by_english_comma  + '&ts=' + str(timestamp))
        rv_data = json.loads(rv.data)
        self.assertTrue(rv_data['angry'][1]>0, 'two keywords split by english comma should have angry count')
        self.assertTrue(rv_data['happy'][1]>0, 'two keywords split by english comma should have happy count')
        self.assertTrue(rv_data['sad'][1]>0, 'two keywords split by english comma should have sad count')

        rv_chunjie = self.app.get('/moodlens/data/global/?query=春节' + '&ts=' + str(timestamp))
        rv_lianghui = self.app.get('/moodlens/data/global/?query=两会' + '&ts=' + str(timestamp))
        rv_chunjie = json.loads(rv_chunjie.data)
        rv_lianghui = json.loads(rv_lianghui.data)
        self.assertNotEqual(rv_chunjie['angry'][1], rv_lianghui['angry'][1], 'lianghui and chunjie shoud have unequal angry count')

    def test_keywords_data(self):
        query_str = '春节'
        timestamp = date2ts('2013-02-09 02:00:00')
        rv = self.app.get('/moodlens/keywords_data/global/?query=' + query_str + '&ts=' + str(timestamp))
        rv_data = json.loads(rv.data)
        stpWds = getStopWds('stpwds_linhao_20130826.txt')
        for k, v in rv_data:
            self.assertNotIn(k, stpWds, 'keywords result should not been in stop words set')
        self.assertTrue(len(rv_data) == 50, 'keywords data should have 50 words')

    def test_weibos_data(self):
        query_str = '春节'
        timestamp = date2ts('2013-02-09 02:00:00')
        rv = self.app.get('/moodlens/weibos_data/happy/global/?query=' + query_str + '&ts=' + str(timestamp))
        rv_data = json.loads(rv.data)
        iter_count = 0
        for emotion, name, user_link, text, weibo_link, ts, reposts_count, retweeted_text in rv_data:
            if iter_count == 0:
                #self.assertTrue(reposts_count>0, 'weibo reposts count should greater than 0')
                iter_count += 1
                temp = reposts_count
                continue    
            self.assertTrue(reposts_count<=temp, 'weibo data should sorted in reposts_count')
            #self.assertTrue(reposts_count>0, 'weibo reposts count should greater than 0')
            temp = reposts_count            
            iter_count += 1

    def test_getPeaks(self):
        query_str = '春节'
        total_days = 30
        during = 24 * 3600
        endts = date2ts('2013-02-09 02:00:00')
        happy_list = []
        angry_list = []
        sad_list = []
        ts_list = []
        for i in range(1, total_days+1):
            rv = self.app.get('/moodlens/data/global/?query=' + query_str  + '&ts=' + str(endts))
            endts = endts - during
            rv_data = json.loads(rv.data)
            if rv_data['happy'][1] + rv_data['angry'][1] + rv_data['sad'][1] > 0:
                t_count = rv_data['happy'][1] + rv_data['angry'][1] + rv_data['sad'][1]
                happy_list.append(str(rv_data['happy'][1]*1.0 / t_count))
                angry_list.append(str(rv_data['angry'][1]*1.0 / t_count))
                sad_list.append(str(rv_data['sad'][1]*1.0 / t_count))
                ts_list.append(str(rv_data['happy'][0] / 1000))
        peak_url = '/moodlens/emotionpeak/?happy=' + ','.join(happy_list) + '&angry=' + ','.join(angry_list) + '&sad=' + ','.join(sad_list) + '&ts=' + ','.join(ts_list) + '&query=' + query_str
        rv = self.app.get(peak_url)
        rv_data = json.loads(rv.data)
        for k, v in rv_data.iteritems():
            self.assertTrue('title' in v and 'text' in v and 'ts' in v, 'title, text, ts should have been in results value')
    '''

if __name__ == '__main__':
    unittest.main()