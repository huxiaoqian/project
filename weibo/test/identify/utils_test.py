# -*- coding: utf-8 -*-

import unittest

import sys
sys.path.append('../../identify')

from utils import acquire_user_by_id, load_scws, cut

from time_utils import unix2hadoop_date, ts2datetime, datetime2ts

from hadoop_utils import generate_job_id

class UtilsTestCase(unittest.TestCase):
    def setUp(self):
        self.test_uid = 1182389073
        self.test_name = u'任志强'

    def test_acquire_user_by_id(self):
        user = acquire_user_by_id('area', self.test_uid)
        username = user['name'].decode('utf-8')
        self.assertNotEqual(username, None, 'search user not exist')
        self.assertEqual(username, self.test_name, 'search wrong user in Xapian')
    
    def test_scws(self):
        sentence = u'中国好声音'
        s = load_scws()
        tokens = cut(s, sentence.encode('utf-8'))
        self.assertNotEqual(tokens, None, 'scws failed')

    def test_time(self):
        date = '2013-03-01'
        ts = datetime2ts(date)
        test_date = ts2datetime(ts)
        self.assertEqual(date, test_date, 'wrong datetime format')

    def test_hadoop_job_id(self):
        date = '2013-03-01'
        ts = datetime2ts(date)
        window_size = 1
        topic_id = 1
        job_id = generate_job_id(ts, window_size, topic_id)
        self.assertEqual(job_id, '2013_03_01_1_1', 'wrong job id')        

if __name__ == '__main__':
    unittest.main()
