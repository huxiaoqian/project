# -*- coding: utf-8 -*-

import sys
import time
import shutil
from utils import check_xapian_completed
from config import DYNAMIC_XAPIAN_WEIBO_STUB_PATH


def check_copy_stub_file():
    # check if xapian data is ready
    xapian_is_completed = check_xapian_completed(now_datestr)
    while not xapian_is_completed:
        print '%s xapian data is not ready, sleep 60 seconds' % now_datestr
        time.sleep(60)
        xapian_is_completed = check_xapian_completed(now_datestr)
    
    # copy stub file
    print '%s xapian data is ready, copy stub file...' % now_datestr
    shutil.copy2('/media/data/stub/master_timeline_weibo_csv_%s' % stub_create_datestr, '%s%s' % (DYNAMIC_XAPIAN_WEIBO_STUB_PATH, now_datestr))
    print '%s copy stub file completed' % now_datestr


if __name__ == '__main__':
    now_datestr = sys.argv[1]
    stub_create_datestr = sys.argv[2]
    check_copy_stub_file()
