# -*- coding: utf-8 -*-


import time
from time_utils import datetime2ts, datetimezero2ts
from config import cron_start, SIMULATE_BASE_DATE
from cron_check import _check_run_customize_topic


base_time = datetimezero2ts(SIMULATE_BASE_DATE)
simulate_now_ts = datetime2ts(cron_start) + time.time() - base_time 

_check_run_customize_topic(simulate_now_ts)