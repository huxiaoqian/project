# -*- coding: utf-8 -*-

import time
import csv
import json
import sys
from datetime import datetime
from datetime import date
from utils import local2datetime, ts2datetime, time2ts

try:
    from xapian_weibo.utils import load_scws, cut
    s = load_scws()
except:
    pass

from config import db
import model
from model import *
from sqlalchemy.exc import IntegrityError

from getReadonlyDB import getReadonlyDB
db_master_timeline = getReadonlyDB('master_timeline')

user_keys = ['_id', 'name', 'gender', 'location',
             'description', 'verified', 'followers_count',
             'statuses_count', 'friends_count', 'profile_image_url',
             'bi_followers_count', 'created_at']


def load_user_by_uid(uidnumber):
    if db.session.query(User).filter_by(id=uidnumber).count():
        return True
    
    user = db_master_timeline.master_timeline_user.find_one({'_id': uidnumber})
    if user:
        try:
            uid = user['_id']
        except KeyError,e:
            return False
        user_item = []
        for k in user_keys:
            try:
                item = user[k]
            except KeyError, e:
                item = None
            user_item.append(item)
    else:
        return False
    

    user_ = User(id=user_item[0], userName=user_item[1], statusesCount=user_item[7], followersCount=user_item[6],
                friendsCount=user_item[8], location=user_item[3], profileImageUrl=user_item[9],
                description=user_item[4], verified=user_item[5], biFollowersCount=user_item[10], gender=user_item[2],
                createdAt=local2datetime(user_item[11]))
    print user_
##    db.session.add(user_)
##    db.session.commit()
    return 'success to insert user %s information' % uidnumber

##def load_user_profile_word(wordsfile, uidnumber=1813080181):
##    f = open(wordsfile, 'r')
##    count = 0
##    for line in f.readlines():
##        mid, uid, datestr, text, retweeted_text = line.strip().split('\t')
##        text = json.loads(text)
##        retweeted_text = json.loads(retweeted_text)
##        mid = eval(mid)
##        uid = eval(uid)        
##        item = Words(id=mid, uid=uid, postDate=local2datetime(datestr), textWord=','.join(text), retweetedTextWord=','.join(retweeted_text))
##        db.session.add(item)
##        if count % 10 == 0:
##            print count, 'commit'
##            db.session.commit()

def load_word_by_time(starttime="2013-03-01", endtime="2013-04-20"):    
    cursor = db_master_timeline.master_timeline_weibo.\
    find({"timestamp": {'$gte': time2ts(starttime),
                        '$lte': time2ts(endtime)}})

    result_count = db_master_timeline.master_timeline_weibo.\
    find({"timestamp": {'$gte': time2ts(starttime),
                        '$lte': time2ts(endtime)}}).count()
    print 'hits result from mongodb: ', result_count
    count = 0
    hit_count = 0
    mids = set()
    
    for weibo in cursor:
        count += 1        
        try:
            mid = weibo["mid"]
            if mid not in mids:
                mids.add(mid)
            else:
                continue
            
            post_ts = weibo["timestamp"]
            text = weibo["text"].encode('utf-8')
            uid = weibo['user']['id']
            text_list = cut(s, text, f=['n'])
            text_words = ','.join(text_list)

            if "retweeted_status" not in weibo or "mid" not in weibo["retweeted_status"]:
                try:
                    db.session.add(Words(id=mid, uid=uid, postDate=ts2datetime(post_ts), textWord=text_words))
                    db.session.commit()
                    hit_count += 1
                except IntegrityError:
                    db.session.rollback()
            else:
                retweeted_mid = weibo["retweeted_status"]["mid"]
                try:
                    db.session.add(Words(id=mid, uid=uid, postDate=ts2datetime(post_ts), textWord=text_words, retweetedMid=retweeted_mid))
                    db.session.commit()
                    hit_count += 1
                except IntegrityError:
                    db.session.rollback()
                if retweeted_mid not in mids:
                    mids.add(retweeted_mid)
                else:
                    continue
                retweeted_ts = weibo["retweeted_status"]["timestamp"]
                retweeted_text = weibo["retweeted_status"]["text"].encode('utf-8')
                retweeted_uid = weibo["retweeted_status"]["user"]["id"]
                rt_text_list = cut(s, retweeted_text, f=['n'])
                rt_text_words = ','.join(rt_text_list)
                try:
                    db.session.add(Words(id=retweeted_mid, uid=retweeted_uid, postDate=ts2datetime(retweeted_ts), textWord=rt_text_words))
                    db.session.commit()
                    hit_count += 1
                except IntegrityError:
                    db.session.rollback()
                
        except KeyError,e:
            continue
        
        if count % 10000 == 0:
            print 'count: ', count, ' hit count: ', hit_count
            
        
    
def load_word():
    from sqlalchemy import create_engine, MetaData, Table

    engine = create_engine('mysql+mysqldb://root:@localhost/weibo?charset=utf8', echo=False, encoding='utf-8')
    metadata = MetaData(bind=engine)
    
    con = engine.connect()
    Words = Table('words', metadata, autoload=True)
    if con.execute('select count(*) from words').first()[0] > 0:
        con.execute(Words.delete())
        
    db_master_timeline = getReadonlyDB('master_timeline')
    cursor = db_master_timeline.master_timeline_weibo.find()
    count = 0
    mids = set()
    for weibo in cursor:
        count += 1
        if count < 0:
            continue
        if count % 10000 == 0:
            print 'count: ', count
        try:
            mid = weibo["mid"]
            if mid not in mids:
                mids.add(mid)
            else:
                continue
            
            post_ts = weibo["timestamp"]
            text = weibo["text"].encode('utf-8')
            uid = weibo['user']['id']
            text_list = cut(s, text, f=['n'])
            text_words = ','.join(text_list)

            if "retweeted_status" not in weibo or "mid" not in weibo["retweeted_status"]:
                con.execute(Words.insert(), id=mid, uid=uid, postDate=ts2datetime(post_ts), textWord=text_words)
            else:
                retweeted_mid = weibo["retweeted_status"]["mid"]
                con.execute(Words.insert(), id=mid, uid=uid, postDate=ts2datetime(post_ts), textWord=text_words, retweetedMid=retweeted_mid)
                if retweeted_mid not in mids:
                    mids.add(retweeted_mid)
                else:
                    continue
                retweeted_ts = weibo["retweeted_status"]["timestamp"]
                retweeted_text = weibo["retweeted_status"]["text"].encode('utf-8')
                retweeted_uid = weibo["retweeted_status"]["user"]["id"]
                rt_text_list = cut(s, retweeted_text, f=['n'])
                rt_text_words = ','.join(rt_text_list)
                con.execute(Words.insert(), id=retweeted_mid, uid=retweeted_uid, postDate=ts2datetime(retweeted_ts), textWord=rt_text_words)
                
        except KeyError,e:
            continue
        
    con.close()

def load_province():
    province_list = (
     (1, '北京'),
     (2, '上海'),
     (3, '香港'),
     (4, '台湾'),
     (5, '重庆'),
     (6, '澳门'),
     (7, '天津'),
     (8, '江苏'),
     (9, '浙江'),
     (10, '四川'),
     (11, '江西'),
     (12, '福建'),
     (13, '青海'),
     (14, '吉林'),
     (15, '贵州'),
     (16, '陕西'),
     (17, '山西'),
     (18, '河北'),
     (19, '湖北'),
     (20, '辽宁'),
     (21, '湖南'),
     (22, '山东'),
     (23, '云南'),
     (24, '河南'),
     (25, '广东'),
     (26, '安徽'),
     (27, '甘肃'),
     (28, '海南'),
     (29, '黑龙江'),
     (30, '内蒙古'),
     (31, '新疆'),
     (32, '广西'),
     (33, '宁夏'),
     (34, '西藏')
    )
    con = engine.connect()
    Province = Table('province', metadata, autoload=True)
    for index, province_name in province_list:
        con.execute(Province.insert(), id=index, province=province_name)
    con.close()

def load_count_range(nrange=10):
    from sqlalchemy import create_engine, MetaData, Table

    engine = create_engine('mysql+mysqldb://root:@localhost/weibo?charset=utf8', echo=False, encoding='utf-8')
    metadata = MetaData(bind=engine)
    
    con = engine.connect()
    RangeCount = Table('range_count', metadata, autoload=True)
    if con.execute('select count(*) from range_count').first()[0] > 0:
        con.execute(RangeCount.delete())
    result_statuses = con.execute('select max(statusesCount) from user').first()
    result_friends = con.execute('select max(friendsCount) from user').first()
    result_followers = con.execute('select max(followersCount) from user').first()
    max_statuses = result_statuses[0]
    max_friends = result_friends[0]
    max_followers = result_followers[0]

    statuses_range = calculate_interval(max_statuses, nrange)
    friends_range = calculate_interval(max_friends, nrange)
    followers_range = calculate_interval(max_followers, nrange)

    
    result_range = [statuses_range, friends_range, followers_range]
    for idx,counttype in enumerate(['statuses', 'friends', 'followers']):
        for x,count in result_range[idx]:
            if x > 0:
                con.execute(RangeCount.insert(), index=x+1, countType=counttype, lowBound=result_range[idx][x-1][1], upBound=count)
            else:
                con.execute(RangeCount.insert(), index=x+1, countType=counttype, lowBound=0, upBound=count)
    con.close()

def calculate_interval(maxcount, nrange):    
    result = []
    
    countinterval = maxcount / nrange
    for x in xrange(0, nrange):
        result.append((x,countinterval * (x+1)))

    return result

def load_field():    
    field_en_name = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology']
    field_zh_name = ['文化', '教育', '娱乐', '时尚', '财经', '媒体', '体育', '科技']
    for i,fieldname in enumerate(field_zh_name):
        field = FieldProfile(fieldEnName=field_en_name[i], fieldZhName=fieldname)
        db.session.add(field)
    db.session.commit()


def readUidArea():
    uid_field = {}
    f = open("./data/r_classsification.txt")
    for line in f.readlines():
        linelist = line.strip().split(' ')
        for ele in linelist:
            try:
                uid, field = ele.split(':')
                uid_field[long(uid)] = field.split(',')
            except:
                continue
    print 'total user field record count: ', len(uid_field.keys())
    return uid_field
    

def load_user_field():    
    uid_field = readUidArea()
    count = 0
    for uid, field in uid_field.items():
        try:
            item = UserField(uid=uid, updateTime=date.fromtimestamp(time.time()), fieldFirst=field[0], fieldSecond=field[1])
        except IndexError,e:
            item = UserField(uid=uid, updateTime=date.fromtimestamp(time.time()), fieldFirst=field[0])
        db.session.add(item)
        count += 1
        if count % 10000 == 0:
            db.session.commit()

def load_user_friendship():
    '''从master_timeline.master_timeline_user更新用户关注关系数据
    '''
    cursor = db_master_timeline.master_timeline_user.find({'$or': [{'friends': {'$ne':[]}}, {'followers': {'$ne':[]}}]})
    total_count = db_master_timeline.master_timeline_user.find({'$or': [{'friends': {'$ne':[]}}, {'followers': {'$ne':[]}}]}).count()

    print 'hit count from mongodb: ', total_count
    
    count = 0
    hit_friends = 0
    hit_followers = 0
    uid_fid = set()
    for user in cursor:
        friends = user['friends']
        followers = user['followers']
        uid = user['id']
        if uid:
            if friends and len(friends) > 0:
                for friend in friends:
                    uid_fid.add((uid, friend)) 
                    if not db.session.query(FollowRelationship).filter_by(uid=uid, fid=friend).all():
                        db.session.add(FollowRelationship(uid=uid, fid=friend))
                        db.session.flush()
                        if hit_friends % 10 == 0:
                            print 'hit friends: ',hit_friends
                        hit_friends += 1
            if followers and len(followers) > 0:
                for follower in followers:
                    uid_fid.add((follower, uid)) 
                    if not db.session.query(FollowRelationship).filter_by(uid=follower, fid=uid).all():
                        db.session.add(FollowRelationship(uid=follower, fid=uid))
                        db.session.flush()
                        if hit_followers % 10 == 0:
                            print 'hit followers: ',hit_followers
                        hit_followers += 1
        db.session.commit()
        if count % 100000 == 0:
            print 'count: ', count
            print 'hit friends count: ', hit_friends
            print 'hit followers count: ', hit_followers
            #db.session.commit()
        if count == total_count - 1:
            print 'over: ', count
##            db.session.commit()
        count += 1
    print len(uid_fid)

def load_personal_burst_word(starttime, endtime):
    from utils import hot_uid_by_word
    from burst_word import burst_model

    gt_count = 50
    uids = hot_uid_by_word(starttime, endtime, gt_count)
    print 'Find uids post more than %s weibo: ' % gt_count, len(uids)
    count = 0
    for uid in uids:
        try:
            burst_model(starttime, endtime, uid)
            count += 1
            if count % 10 == 0:
                print count
        except:
            count += 1
            continue
    
def main():
    pass
    #load_province()
    #load_count_range()
    #load_word()
    #load_word_by_time("2013-03-01", "2013-04-20")
    #load_word_by_time("2013-01-01", "2013-03-01")
    #load_user_by_uid()
    #load_field()
    #load_user_field()
    #load_user_friendship()
    #load_repost_relation()
    #load_personal_burst_word('2013-04-08','2013-04-15')
    #load_personal_burst_word('2013-04-01','2013-04-08')
    load_personal_burst_word('2013-03-25','2013-04-01')
    load_personal_burst_word('2013-03-18','2013-03-25')
    load_personal_burst_word('2013-03-11','2013-04-18')
    load_personal_burst_word('2013-03-04','2013-04-11')
    
if __name__ == '__main__': main()
            
