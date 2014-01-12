# -*- coding: utf-8 -*-

import os
import json
import leveldb
from weibo.global_config import LEVELDBPATH


def getProfilePersonDbByDate(datestr):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_%s' % datestr), \
                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_db


def getPersonData(uid, datestr):
    level = getProfilePersonDbByDate(datestr)
    try:
        results = level.Get(str(uid))
        active, important, reposts, original, emoticon, interact_dict, keywords_dict = results.split('_\/')
        active = int(active)
        important = int(important)
        reposts = int(reposts)
        original = int(original)
        emoticon = int(emoticon)
        interact_dict = json.loads(interact_dict)
        direct_interact = interact_dict['direct']
        retweeted_interact = interact_dict['retweeted']
        keywords_dict = json.loads(keywords_dict)
    except KeyError:
        active = important = reposts = original = emoticon = 0
        direct_interact = {}
        retweeted_interact = {}
        keywords_dict = {}

    return active, important, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict


def main():
    global_level = getProfilePersonDbByDate('20130901')
    for k, v in global_level.RangeIter():
        print k, v


if __name__ == '__main__':
    main()
