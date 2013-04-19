# -*- coding: utf-8 -*-

import time

def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def window2time(window, size=24*60*60):
    return window*size
