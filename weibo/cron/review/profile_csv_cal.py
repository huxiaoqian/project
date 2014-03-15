# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import json
import leveldb
#import LEVELDBPATH
from xapian_weibo.utils import load_scws, cut
from xapian_weibo.csv2json import itemLine2Dict

LEVELDBPATH = '/home/mirage/leveldb_20140314/'

# init scws
scws = load_scws()

# init seed_set
seed_set = set([])
with open('official_emoticons.txt') as f:
    for l in f:
        seed_set.add(l.rstrip())


def emoticon_find(text):
    emotion_pattern = r'\[(\S+?)\]'
    remotions = re.findall(emotion_pattern, text)

    emoticons = []
    haveEmoticon = False
    if remotions:
        for e in remotions:
            if e in seed_set:
                haveEmoticon = True
                break
    
    return haveEmoticon


def _utf_encode(s):
    if isinstance(s, unicode):
        return s.encode('utf-8', 'ignore')
    else:
        return s


def profile_person_cal(itemdict):
    uid = itemdict['user']
    retweeted_mid = itemdict['retweeted_mid']
    reposts_count = itemdict['reposts_count']
    comments_count = itemdict['comments_count']
    text = itemdict['text']
    retweeted_uid = itemdict['retweeted_uid']

    try:
        keywords_dict = json.loads(daily_profile_keywords_db.Get(str(uid)))
    except:
    	  keywords_dict = {}

    try:
        interact_dict = json.loads(daily_profile_interact_db.Get(str(uid)))
        direct_interact = interact_dict['direct']
        retweeted_interact = interact_dict['retweeted']
    except:
    	direct_interact = {}
    	retweeted_interact = {}
    
    try:
    	active, important, reposts, original, emoticon = daily_profile_counts_db.Get(str(uid)).split('_\/')
        active = int(active)
        important = int(important)
        reposts = int(reposts)
        original = int(original)
        emoticon = int(emoticon)
    except KeyError:
        active = important = reposts = original = emoticon = 0

    active += 1
    important += reposts_count
    
    if retweeted_mid != 0:
        reposts += 1
    else:
        original += 1

    _emoticons = emoticon_find(text)
    if _emoticons:
        emoticon += 1
    
    if isinstance(text, str):
        text = text.decode('utf-8', 'ignore')

    RE = re.compile(u'//@([a-zA-Z-_⺀-⺙⺛-⻳⼀-⿕々〇〡-〩〸-〺〻㐀-䶵一-鿃豈-鶴侮-頻並-龎]+):', re.UNICODE)
    repost_users = RE.findall(text)

    if len(repost_users):
        repost_user = repost_users[0]
        try:
            direct_interact[repost_user] += 1
        except KeyError:
            direct_interact[repost_user] = 1

    if retweeted_uid != 0:
        try:
            retweeted_interact[retweeted_uid] += 1
        except KeyError:
            retweeted_interact[retweeted_uid] = 1

    interact_dict = {'direct': direct_interact, 'retweeted': retweeted_interact}

    terms = cut(scws, _utf_encode(text), f='n')
    for term in terms:
        try:
            keywords_dict[term] += 1
        except KeyError:
            keywords_dict[term] = 1
    
    daily_profile_keywords_db.Put(str(uid), json.dumps(keywords_dict))
    daily_profile_interact_db.Put(str(uid), json.dumps(interact_dict))
    daily_profile_counts_db.Put(str(uid), '_\/'.join([str(active), str(important), str(reposts), str(original), str(emoticon)]))


if __name__ == '__main__':
    now_datestr = sys.argv[1]

    daily_profile_keywords_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_keywords_%s' % now_datestr),
                                                block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    daily_profile_interact_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_interact_%s' % now_datestr),
                                                block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    daily_profile_counts_db =  leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_counts_%s' % now_datestr),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    csv_dir_path = '/home/mirage/dev/original_data/csv/'
    source_path = csv_dir_path + '%s/' % now_datestr
    source_files = os.listdir(source_path)
    
    count = 0
    ts = te = time.time()
    for f in source_files:
        print f
    	f = open(source_path + f, 'r')
    	for line in f:
    	    itemdict = itemLine2Dict(line)
            if itemdict:
                profile_person_cal(itemdict)
            
                if count % 10000 == 0:
                    te = time.time()
                    print count, '%s sec' % (te - ts), 'profile_person_cal', now_datestr
                    ts = te
                count += 1
                
