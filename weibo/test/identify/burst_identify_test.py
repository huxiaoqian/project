# -*- coding: utf-8 -*-

import sys
import time
import unittest

sys.path.append('../../identify')

from burst import followers_rank, active_rank, important_rank

from burst_monitor import realtime_burst_user

from time_utils import ts2datetime, datetime2ts

class BurstTestCase(unittest.TestCase):
    def setUp(self):
        self.top_n = 500
        self.date = '2013-3-1'
        self.window_size = 1
        
    def test_followers_rank(self):
        data = followers_rank(self.top_n, self.date, self.window_size)
        self.assertIsNotNone(data, 'none sorted ids')

    def test_active_rank(self):
        data = active_rank(self.top_n, self.date, self.window_size)
        self.assertIsNotNone(data, 'none sorted ids')

    def test_important_rank(self):
        data = important_rank(self.top_n, self.date, self.window_size)
        self.assertIsNotNone(data, 'none sorted ids')

    def test_realtime_burst_user(self):
        # current_time = time.time()
        current_time = datetime2ts('2013-3-7') + 12*60*60
        data = realtime_burst_user(self.top_n, current_time)
        self.assertIsNotNone(data, 'none sorted ids')

if __name__ == '__main__':
    unittest.main()
