# -*- coding: utf-8 -*-

import os
import shutil
import leveldb
from config import LEVELDBPATH

def clear_profile_person_keywords_db():
    files_names_list = os.listdir(os.path.join(LEVELDBPATH, 'keywords'))
    for f in files_names_list:
        if f[24:32] != now_datestr:
            continue

        shutil.rmtree(os.path.join(LEVELDBPATH, 'keywords/%s' % f))


if __name__ == '__main__':
	  now_datestr = '20140312'
	  clear_profile_person_keywords_db()
