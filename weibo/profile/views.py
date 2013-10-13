# -*- coding: utf-8 -*-
from gensim import corpora, models, similarities
#new
from flask import Flask, url_for, render_template, request, make_response, flash, abort, Blueprint, session, redirect

from weibo_trend import getBurstWord, getCount
#new
from config import app, db
import os
import sys
import urllib2
import datetime
from datetime import date
import operator, random

from operator import itemgetter
import re
from city_color import province_color_map, getProvince
from flask.ext import admin
import model
from utils import acquire_topic_id, read_rank_results, pagerank_rank, degree_rank, make_network_graph, get_above100_weibos, weiboinfo2url
from time_utils import ts2datetime, datetime2ts, window2time, ts2HMS
from utils import top_keywords
from xapian_weibo.xapian_backend import XapianSearch
#new
import time
try:
    import simplejosn as json
except ImportError:
    import json

from weibo.extensions import db
from weibo.model import *
import weibo.model
import json

from utils import hot_uid_by_word, last_week, last_month, ts2date, getFieldUsersByScores, time2ts, datetime2ts
from flask.ext.sqlalchemy import Pagination

import leveldb, os, operator
#new

from xapian_weibo.xapian_backend import XapianSearch

xapian_search_user = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
xapian_search_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
xapian_search_sentiment = XapianSearch(path='/opt/xapian_weibo/data/20130807', name='master_timeline_sentiment', schema_version=3)
#new
LEVELDBPATH = '/home/mirage/leveldb'
buckets = {}
emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
emotions_zh_kv = {'happy': '高兴', 'angry': '愤怒', 'sad': '悲伤'}
fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology']


def get_bucket(bucket):
    if bucket in buckets:
        return buckets[bucket]
    buckets[bucket] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_' + bucket), block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return buckets[bucket]

def getSampleUsers():
    f = open('user_timeline_0516.txt', 'r')
    uidlist = []
    for line in f.readlines():
        uid = int(line.strip().split(' ')[0])
        uidlist.append(uid)
    return {'uids': uidlist, 'length': len(uidlist)}
#new

def fieldsEn2Zh(name):
    if name == 'finance':
        return '财经'
    if name == 'media':
        return '媒体'
    if name == 'culture':
        return '文化'
    if name == 'technology':
        return '科技'
    if name == 'entertainment':
        return '娱乐'
    if name == 'education':
        return '教育'
    if name == 'fashion':
        return '时尚'
    if name == 'sports':
        return '体育'

COUNT_PER_PAGE = 20

def getStaticInfo():
    statuscount = db.session.query(RangeCount).filter(RangeCount.countType=='statuses').all()
    friendscount = db.session.query(RangeCount).filter(RangeCount.countType=='friends').all()
    followerscount = db.session.query(RangeCount).filter(RangeCount.countType=='followers').all()
    province = db.session.query(Province).order_by(Province.id).all()
    field = db.session.query(FieldProfile).all()
    return statuscount, friendscount, followerscount, province, field


def yymInfo(uid):
    query_dict = {
        '_id': int(uid)
    }
    count, get_results = xapian_search_user.search(query=query_dict, fields=['created_at', '_id', 'name', \
        'statuses_count', 'followers_count', 'friends_count', 'description', 'profile_image_url', 'verified', 'gender'])
    user = None
    if count > 0:
        for r in get_results():
            statusesCount = r['statuses_count']
            followersCount = r['followers_count']
            friendsCount = r['friends_count']
            userName = r['name']
            description = r['description']
            uid = r['_id']
            profileImageUrl = r['profile_image_url']
            verified = r['verified']
            gender = r['gender']
            user = {'id': uid, 'userName': userName, 'statusesCount': statusesCount, 'followersCount': \
            followersCount, 'friendsCount': friendsCount, 'description': description, 'profileImageUrl': profileImageUrl,
            'verified': verified, 'gender': gender}
            return user
    return user
    
mod = Blueprint('profile', __name__, url_prefix='/profile')   

@mod.route('/log_in', methods=['GET','POST'])
def log_in():
    session['logged_in'] = request.form['log_in']
    session['user'] = request.form['user']
    if 'logged_in' in session and session['logged_in']:
        return json.dumps('Right')
    else:
        return json.dumps('Wrong')

@mod.route('/search/', methods=['GET', 'POST'])
@mod.route('/search/<model>', methods=['GET', 'POST'])
def profile_search(model='hotest'):
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == 'GET':
                statuscount, friendscount, followerscount, province, field = getStaticInfo()
                if model == 'person':
                    nickname = urllib2.unquote(request.args.get('nickname'))
                    return render_template('profile/profile_search.html',statuscount=statuscount,
                                           friendscount=friendscount, followerscount=followerscount,
                                           location=province, field=field, model=model, result=None, nickname=nickname)
                elif model in ['statuses', 'friends', 'followers']:
                    low = int(request.args.get('low'))
                    up = int(request.args.get('up'))
                    return render_template('profile/profile_search.html',statuscount=statuscount,
                                           friendscount=friendscount, followerscount=followerscount,
                                           location=province, field=field, model=model, result=None, low=low, up=up)
                elif model == 'province':
                    province_arg = request.args.get('province')
                    return render_template('profile/profile_search.html',statuscount=statuscount,
                                           friendscount=friendscount, followerscount=followerscount,
                                           location=province, field=field, model=model, result=None, province=province_arg)
                else:
                    return render_template('profile/profile_search.html',statuscount=statuscount,
                                           friendscount=friendscount, followerscount=followerscount,
                                           location=province, field=field, model=model, result=None)
            if request.method == 'POST' and request.form['page']:
                if model == 'newest':
                    page = int(request.form['page'])
                    if page == 1:
                        startoffset = 0
                    else:
                        startoffset = (page - 1) * COUNT_PER_PAGE

                    total_days = 90
                    today = datetime.datetime.today()
                    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
                    now_ts = int(now_ts)
                    during = 24 * 3600
                    begin_ts = now_ts - total_days * during

                    query_dict = {
                        'created_at': {
                            '$gt': begin_ts,
                            '$lt': now_ts,
                        }
                    }
                    count, get_results = xapian_search_user.search(query=query_dict, start_offset=startoffset, max_offset=COUNT_PER_PAGE,
                                                       fields=['created_at', '_id', 'name', 'statuses_count', 'followers_count', 'friends_count', 'description', 'profile_image_url'], 
                                                       sort_by=['-created_at'])
                    users = []
                    for r in get_results():
                        statusesCount = r['statuses_count']
                        followersCount = r['followers_count']
                        friendsCount = r['friends_count']
                        userName = r['name']
                        description = r['description']
                        uid = r['_id']
                        profileImageUrl = r['profile_image_url']
                        users.append({'id': uid, 'userName': userName, 'statusesCount': statusesCount, 'followersCount': followersCount, 'friendsCount': friendsCount,
                                  'description': description, 'profileImageUrl': profileImageUrl})
                    return json.dumps(users)
                elif model == 'hotest':
                    from utils import active_rank
                    current_time = '2013-3-7'
                    window_size = 1
                    top_n = 2000
                    rank_results = active_rank(top_n, current_time, window_size)
                    users = []
                    page = int(request.form['page'])
                    if page == 1:
                        startoffset = 0
                    else:
                        startoffset = (page - 1) * COUNT_PER_PAGE
                    endoffset = startoffset + COUNT_PER_PAGE - 1
                    for uid in rank_results:
                        user = getUserInfoById(uid)
                        if user:
                            users.append(user)
                    return json.dumps(users[startoffset:endoffset])
                elif db.session.query(FieldProfile).filter(FieldProfile.fieldEnName==model).count():
                    page = int(request.form['page'])
                    if page == 1:
                        startoffset = 0
                    else:
                        startoffset = (page - 1) * COUNT_PER_PAGE
                    endoffset = startoffset + COUNT_PER_PAGE - 1
                    uids = getFieldUsersByScores(model, startoffset, endoffset)
                    uidlist = [int(uid) for uid in uids]
                    users = User.query.filter(User.id.in_(uidlist)).all()
                    return json.dumps([i.serialize for i in users])
                elif model == 'person':
                    nickname = urllib2.unquote(request.form['nickname'])
                    users = User.query.filter(User.userName==nickname).all()
                    return json.dumps([i.serialize for i in users])
                elif model in ['statuses', 'friends', 'followers']:
                    low = int(request.form['low'])
                    up = int(request.form['up'])
                    page = int(request.form['page'])
                    if page == 1:
                        startoffset = 0
                    else:
                        startoffset = (page - 1) * COUNT_PER_PAGE
                    sorted_key = '%s_count' % model
                    query_dict = { sorted_key: {
                            '$gt': low,
                            '$lt': up,
                        }
                    }
                    count, get_results = xapian_search_user.search(query=query_dict, start_offset=startoffset, max_offset=COUNT_PER_PAGE,
                                                       fields=['_id', 'name', 'statuses_count', 'followers_count', 'friends_count', 'description', 'profile_image_url'], 
                                                       sort_by=['-' + sorted_key])
                    users = []
                    for r in get_results():
                        statusesCount = r['statuses_count']
                        followersCount = r['followers_count']
                        friendsCount = r['friends_count']
                        userName = r['name']
                        description = r['description']
                        uid = r['_id']
                        profileImageUrl = r['profile_image_url']
                        users.append({'id': uid, 'userName': userName, 'statusesCount': statusesCount, 'followersCount': followersCount, 'friendsCount': friendsCount,
                                      'description': description, 'profileImageUrl': profileImageUrl})
                    return json.dumps(users)
                elif model == 'province':
                    province = request.form['province']
                    basequery = User.query.filter(User.location.startswith(province)).limit(1000)#.order_by(User.followersCount.desc())
                    page = int(request.form['page'])
                    users = basequery.paginate(page, COUNT_PER_PAGE, False).items
                    return json.dumps([i.serialize for i in users])
        else:
            pas = db.session.query(UserList).filter(UserList.id==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.profile
                    if identy == 1:
                        if request.method == 'GET':
                            statuscount, friendscount, followerscount, province, field = getStaticInfo()
                            if model == 'person':
                                nickname = urllib2.unquote(request.args.get('nickname'))
                                return render_template('profile/profile_search.html',statuscount=statuscount,
                                                       friendscount=friendscount, followerscount=followerscount,
                                                       location=province, field=field, model=model, result=None, nickname=nickname)
                            elif model in ['statuses', 'friends', 'followers']:
                                low = int(request.args.get('low'))
                                up = int(request.args.get('up'))
                                return render_template('profile/profile_search.html',statuscount=statuscount,
                                                       friendscount=friendscount, followerscount=followerscount,
                                                       location=province, field=field, model=model, result=None, low=low, up=up)
                            elif model == 'province':
                                province_arg = request.args.get('province')
                                return render_template('profile/profile_search.html',statuscount=statuscount,
                                                       friendscount=friendscount, followerscount=followerscount,
                                                       location=province, field=field, model=model, result=None, province=province_arg)
                            else:
                                return render_template('profile/profile_search.html',statuscount=statuscount,
                                                       friendscount=friendscount, followerscount=followerscount,
                                                       location=province, field=field, model=model, result=None)
                        if request.method == 'POST' and request.form['page']:
                            if model == 'newest':
                                page = int(request.form['page'])
                                if page == 1:
                                    startoffset = 0
                                else:
                                    startoffset = (page - 1) * COUNT_PER_PAGE

                                total_days = 90
                                today = datetime.datetime.today()
                                now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
                                now_ts = int(now_ts)
                                during = 24 * 3600
                                begin_ts = now_ts - total_days * during

                                query_dict = {
                                    'created_at': {
                                        '$gt': begin_ts,
                                        '$lt': now_ts,
                                    }
                                }
                                count, get_results = xapian_search_user.search(query=query_dict, start_offset=startoffset, max_offset=COUNT_PER_PAGE,
                                                                   fields=['created_at', '_id', 'name', 'statuses_count', 'followers_count', 'friends_count', 'description', 'profile_image_url'], 
                                                                   sort_by=['-created_at'])
                                users = []
                                for r in get_results():
                                    statusesCount = r['statuses_count']
                                    followersCount = r['followers_count']
                                    friendsCount = r['friends_count']
                                    userName = r['name']
                                    description = r['description']
                                    uid = r['_id']
                                    profileImageUrl = r['profile_image_url']
                                    users.append({'id': uid, 'userName': userName, 'statusesCount': statusesCount, 'followersCount': followersCount, 'friendsCount': friendsCount,
                                              'description': description, 'profileImageUrl': profileImageUrl})
                                return json.dumps(users)
                            elif model == 'hotest':
                                from utils import active_rank
                                current_time = '2013-3-7'
                                window_size = 1
                                top_n = 2000
                                rank_results = active_rank(top_n, current_time, window_size)
                                users = []
                                page = int(request.form['page'])
                                if page == 1:
                                    startoffset = 0
                                else:
                                    startoffset = (page - 1) * COUNT_PER_PAGE
                                endoffset = startoffset + COUNT_PER_PAGE - 1
                                for uid in rank_results:
                                    user = getUserInfoById(uid)
                                    if user:
                                        users.append(user)
                                return json.dumps(users[startoffset:endoffset])
                            elif db.session.query(FieldProfile).filter(FieldProfile.fieldEnName==model).count():
                                page = int(request.form['page'])
                                if page == 1:
                                    startoffset = 0
                                else:
                                    startoffset = (page - 1) * COUNT_PER_PAGE
                                endoffset = startoffset + COUNT_PER_PAGE - 1
                                uids = getFieldUsersByScores(model, startoffset, endoffset)
                                uidlist = [int(uid) for uid in uids]
                                users = User.query.filter(User.id.in_(uidlist)).all()
                                return json.dumps([i.serialize for i in users])
                            elif model == 'person':
                                nickname = urllib2.unquote(request.form['nickname'])
                                users = User.query.filter(User.userName==nickname).all()
                                return json.dumps([i.serialize for i in users])
                            elif model in ['statuses', 'friends', 'followers']:
                                low = int(request.form['low'])
                                up = int(request.form['up'])
                                page = int(request.form['page'])
                                if page == 1:
                                    startoffset = 0
                                else:
                                    startoffset = (page - 1) * COUNT_PER_PAGE
                                sorted_key = '%s_count' % model
                                query_dict = { sorted_key: {
                                        '$gt': low,
                                        '$lt': up,
                                    }
                                }
                                count, get_results = xapian_search_user.search(query=query_dict, start_offset=startoffset, max_offset=COUNT_PER_PAGE,
                                                                   fields=['_id', 'name', 'statuses_count', 'followers_count', 'friends_count', 'description', 'profile_image_url'], 
                                                                   sort_by=['-' + sorted_key])
                                users = []
                                for r in get_results():
                                    statusesCount = r['statuses_count']
                                    followersCount = r['followers_count']
                                    friendsCount = r['friends_count']
                                    userName = r['name']
                                    description = r['description']
                                    uid = r['_id']
                                    profileImageUrl = r['profile_image_url']
                                    users.append({'id': uid, 'userName': userName, 'statusesCount': statusesCount, 'followersCount': followersCount, 'friendsCount': friendsCount,
                                                  'description': description, 'profileImageUrl': profileImageUrl})
                                return json.dumps(users)
                            elif model == 'province':
                                province = request.form['province']
                                basequery = User.query.filter(User.location.startswith(province)).limit(1000)#.order_by(User.followersCount.desc())
                                page = int(request.form['page'])
                                users = basequery.paginate(page, COUNT_PER_PAGE, False).items
                                return json.dumps([i.serialize for i in users])
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route('/micro_ajax_tab/<module>/', methods=['POST', 'GET'])
def micro_ajax_tab(module):
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == 'GET':
                form = request.args
                query = form.get('query', None)
                startstr = form.get('startdate', None)
                endstr = form.get('enddate', None)

                window_size = (datetime.date.fromtimestamp(time2ts(endstr)) - datetime.date.fromtimestamp(time2ts(startstr))).days
        
                if query:
                    topic_id = acquire_topic_id(query)

                if module == 'identify':
                    return render_template('profile/ajax/micro_identify.html', window_size=window_size, page_num=10, top_n=2000, topic=query, enddate=endstr)
                if module == 'keyweibo':
                    return render_template('profile/ajax/micro_keyweibo.html')
                if module == 'subevent':
                    return render_template('profile/ajax/micro_subevent.html')
                else:
                    pass
        else:
            pas = db.session.query(UserList).filter(UserList.id==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.profile
                    if identy == 1:
                        if request.method == 'GET':
                            form = request.args
                            query = form.get('query', None)
                            startstr = form.get('startdate', None)
                            endstr = form.get('enddate', None)

                            window_size = (datetime.date.fromtimestamp(time2ts(endstr)) - datetime.date.fromtimestamp(time2ts(startstr))).days
        
                            if query:
                                topic_id = acquire_topic_id(query)

                            if module == 'identify':
                                return render_template('profile/ajax/micro_identify.html', window_size=window_size, page_num=10, top_n=2000, topic=query, enddate=endstr)
                            if module == 'keyweibo':
                                return render_template('profile/ajax/micro_keyweibo.html')
                            if module == 'subevent':
                                return render_template('profile/ajax/micro_subevent.html')
                            else:
                                pass
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')
#new
@mod.route('/group/<fieldEnName>', methods=['GET', 'POST'])
def profile_group(fieldEnName):
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            field = FieldProfile.query.all()
            return render_template('profile/profile_group.html', field=field, model=fieldEnName, atfield=unicode(fieldsEn2Zh(fieldEnName), 'utf-8'))
        else:
            pas = db.session.query(UserList).filter(UserList.id==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.profile
                    if identy == 1:
                        field = FieldProfile.query.all()
                        return render_template('profile/profile_group.html', field=field, model=fieldEnName, atfield=unicode(fieldsEn2Zh(fieldEnName), 'utf-8'))
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route('/person/<uid>', methods=['GET', 'POST'])
def profile_person(uid):
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if uid:
                count, get_results = xapian_search_user.search(query={'_id': int(uid)}, fields=['profile_image_url', 'name', 'friends_count', \
                                                  'statuses_count', 'followers_count', 'gender', 'verified', 'created_at', 'location'])
                if count > 0:
                    for r in get_results():
                        user = {'id': uid, 'profile_image_url': r['profile_image_url'], 'userName':  unicode(r['name'], 'utf-8'), 'friends_count': r['friends_count'], \
                                'statuses_count': r['statuses_count'], 'followers_count': r['followers_count'], 'gender': r['gender'], \
                                'verified': r['verified'], 'created_at': r['created_at'], 'location': unicode(r['location'], "utf-8")}
                else:
                    return 'no such user'
            return render_template('profile/profile_person.html', user=user)
        else:
            pas = db.session.query(UserList).filter(UserList.id==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.profile
                    if identy == 1:
                        if uid:
                            count, get_results = xapian_search_user.search(query={'_id': int(uid)}, fields=['profile_image_url', 'name', 'friends_count', \
                                                              'statuses_count', 'followers_count', 'gender', 'verified', 'created_at', 'location'])
                            if count > 0:
                                for r in get_results():
                                    user = {'id': uid, 'profile_image_url': r['profile_image_url'], 'userName':  unicode(r['name'], 'utf-8'), 'friends_count': r['friends_count'], \
                                            'statuses_count': r['statuses_count'], 'followers_count': r['followers_count'], 'gender': r['gender'], \
                                            'verified': r['verified'], 'created_at': r['created_at'], 'location': unicode(r['location'], "utf-8")}
                            else:
                                return 'no such user'
                        return render_template('profile/profile_person.html', user=user)
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')



@mod.route('/person_interact_network/<uid>', methods=['GET', 'POST'])
def profile_interact_network(uid):
    if request.method == 'GET':     
        center_uid = uid
        friendship_bucket = get_bucket('friendship')
        friends_key = str(uid) + '_' + 'friends'
        followers_key = str(uid) + '_' + 'followers'
        fri_fol = []
        try:
            friends = json.loads(friendship_bucket.Get(friends_key))
        except KeyError:
            friends = []
        fri_fol.extend(friends)
        try:
            followers = json.loads(friendship_bucket.Get(followers_key))
        except KeyError:
            followers = []
        fri_fol.extend(followers)

        total_days = 270
        today = datetime.datetime.today()
        now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
        now_ts = int(now_ts)
        during = 24 * 3600

        if request.args.get('interval'):
            total_days =  int(request.args.get('interval')) - 1

        total_days = 270

        interact_bucket = get_bucket('user_daily_interact_count')
        uid_interact_count = {} 
        for i in xrange(-total_days + 1, 1):
            lt = now_ts + during * i
            for f_uid in fri_fol:
                count = 0
                try:
                    count += int(interact_bucket.Get(str(uid) + '_' + str(f_uid) + '_' + str(lt)))
                except KeyError:
                    pass
                try:
                    count += int(interact_bucket.Get(str(f_uid) + '_' + str(uid) + '_' + str(lt)))
                except KeyError:
                    pass
                if count:
                    try:
                        uid_interact_count[str(f_uid)] += count
                    except KeyError:
                        uid_interact_count[str(f_uid)] = count

        sorted_counts = sorted(uid_interact_count.iteritems(), key=operator.itemgetter(1), reverse=True)
     
        top_8_fri = {}
        top_36_fri = {}
        for uid, count in sorted_counts:
            
            if len(top_8_fri) <8:
               top_8_fri[uid] = count
               uid_interact_count.pop(uid, None)
               continue 
            elif len(top_36_fri)<36:
               top_36_fri[uid] = count
               uid_interact_count.pop(uid, None)
               if len(top_36_fri) == 36:
                   break
        def node(friendsCount,followersCount,statusesCount,gender,verified,profileImageUrl,count,id,name):
            return {"children":[],"data":{"friendsCount":friendsCount,"followersCount":followersCount,"statusesCount":statusesCount,"gender":gender,"verified":verified,"profileImageUrl":profileImageUrl,"$color":"#AEA9F8","$angularWidth":1000,"count":count},"id": id,"name": name}
        def unode(uid,name,count):
            return {"children":[],"data":{"$color":"#AEA9F8","$angularWidth":1000,"count":count},"id": uid,"name": name}
            #FCD9A1 AEA9F8 B0AAF6 B2ABF4 B6AEEF E0C7C0 D2BFD0 ECCFB3 D4C0CE 
        def source(uid,name):
            return {"children":[],"data":{"type":"none"},"id": uid,"name": name}
        
        first=source(center_uid ,yymInfo(center_uid)['userName'] )
        second=[]
        third=[]
        order=[8,6,5,4,4,3,3,3]
        allcounts=0
        allorder=0
        flag=0
        for i in top_8_fri:
            allcounts+=top_8_fri[i]

        for i in top_8_fri:
            if(int(top_8_fri[i]*36/allcounts)>=1 ):
                order[flag]=int(top_8_fri[i]*36/allcounts) 
            else:
                order[flag]=1
            flag+=1

        for i in range(0,8):
            allorder+=order[i]

        order[0]+=(36-allorder)

        for i in top_8_fri:
            info=yymInfo(i)
            if(info==None):
                second.append(unode(i,i,top_8_fri[i]))
            else:
                second.append(node(info['friendsCount'],info['followersCount'],info['statusesCount'],info['gender'],info['verified'],info['profileImageUrl'],top_8_fri[i],info['id'],info['userName']))

        for i in range(0,8):
            second[i]['data']['$color']="#B2ABF4"
             
        for i in top_36_fri:
            info=yymInfo(i)
            if(info==None):
                third.append(unode(i,i,top_36_fri[i]))
            else:
                third.append(node(info['friendsCount'],info['followersCount'],info['statusesCount'],info['gender'],info['verified'],info['profileImageUrl'],top_36_fri[i],info['id'],info['userName']))

        sum=0
        for i in range(0,8):
            for k in range(0,order[i]):
                (second[i]['children']).append(third[sum+k])
            sum=sum+order[i]
            
        first['children'] = second
        return json.dumps(first)

@mod.route("/identify/keyweibos/", methods=['POST', 'GET'])
def keyweibos():
    if request.method == 'POST':
        form = request.form
        topic = form.get('topic', None)
        topic_id = form.get('topic_id', None)
        startdate = form.get('startdate', None)
        enddate = form.get('enddate', None)
        page = int(form.get('page', 1))

        start_ts = datetime2ts(startdate)
        end_ts = datetime2ts(enddate)

        if not topic_id:
            if topic:
                topic_id = acquire_topic_id(topic)
        results, pages = get_above100_weibos(topic_id, start_ts, end_ts, page)
        return json.dumps({'results': results, 'pages': pages, 'page': page})

@mod.route("/identify/area/", methods=["GET", "POST"])
def area():
    request_method = request.method
    if request_method == 'POST':
        #form data
        form = request.form

        topic = form.get('topic', None)
        topic_id = form.get('topic_id', None)
        startdate = form.get('startdate', None)
        enddate = form.get('enddate', None)

        top_n = int(form.get('top_n', 2000))
        page_num = int(form.get('page_num', 20))
        window_size = int(form.get('window_size', 1))
        action = form.get('action', None)

        #acquire topic id
        if not topic_id:
            if topic:
                topic_id = acquire_topic_id(topic)
            else:
                pass

        if window_size <= 7:
            rank_method = 'pagerank'
        else:
            rank_method = 'degree'

        current_time = datetime2ts(enddate)

        if action == 'rank':
            current_date = ts2datetime(current_time)
            current_data = read_rank_results(top_n, 'area', rank_method, current_date, window_size, topic_id=topic_id)
            if not current_data:
                if rank_method == 'pagerank':
                    rank_func = pagerank_rank
                    if rank_func:
                        current_data = rank_func(top_n, current_date, topic_id, window_size)
                elif rank_method == 'degree':
                    rank_func = degree_rank
                    if rank_func:
                        current_data = rank_func(top_n, current_date, topic_id, window_size)
            return json.dumps({'status': 'current finished', 'data': current_data})

        elif action == 'check_rank_status':
            #check Hadoop Job Status
            job_id = form.get('job_id', None)
            if not job_id:
                return json.dumps({'error': 'need a job'})
            status = monitor(job_id)
            return json.dumps({'status': status})

        else:
            abort(404)
    else:
        abort(404)

@mod.route("/identify/area/network/", methods=["POST"])
def area_network():
    request_method = request.method
    if request_method == 'POST':
        form = request.form

        window_size = int(form.get('window_size', 1))
        topic = form.get('topic', None)
        topic_id = form.get('topic_id', None)
        enddate = form.get('enddate', None)

        #acquire topic id
        if not topic_id:
            if topic:
                topic_id = acquire_topic_id(topic)
            else:
                pass
                
        current_time = datetime2ts(enddate)
        current_date = ts2datetime(current_time)
        gexf = None
        
        if not topic_id:
            gexf = ''
        else:
            gexf = make_network_graph(current_date, topic_id, window_size)
        response = make_response(gexf)
        response.headers['Content-Type'] = 'text/xml'
        return response
    else:
        abort(404)

@mod.route("/subevent/", methods=["GET", "POST"])
def subevent():
    from events_discovery import events
    if request.method == 'POST':
        form = request.form
        topic = form.get('topic', None)
        topic_id = form.get('topic_id', None)
        startdate = form.get('startdate', None)
        enddate = form.get('enddate', None)

        if not topic_id:
            if topic:
                topic_id = acquire_topic_id(topic)

        event_results = events(topic, startdate, enddate)
        return json.dumps(event_results)
def time2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def ts2datestr(ts):
    return date.fromtimestamp(float(ts)).isoformat()
def search_user_by_mid(mid):
    r_c, r_re = xapian_search_weibo.search(query={'_id': int(mid)}, fields=['user'])
    if r_c:
        for r in r_re():
            uid = r['user']
            return uid
    else:
        return None

def search_uid_by_username(username):
    u_c, u_re = xapian_search_user.search(query={'name': username}, fields=['_id'])
    if u_c:
        for r in u_re():
            uid = r['_id']
            return uid
    else:
        return None

@mod.route('/topic/', methods=['POST', 'GET'])
def index():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            error = None
            if request.method == 'GET':
                return render_template('index.html')
            elif request.method == 'POST':
                form = request.form
                query = form.get('keyword', None)
                level = form.get('level', None)
                startstr = form.get('startdate', None)
                endstr = form.get('enddate', None)

                if query:
                    topic_id = acquire_topic_id(query)
                    if level == 'macroview':
                        return render_template('macroview.html', keyword=query, startdate=startstr, enddate=endstr)
                    elif level == 'microview':
                        return render_template('microview.html', keyword=query, startdate=startstr, enddate=endstr)
                    else:
                        pass
                else:
                    flash(u'请输入事件关键词！')
                    return render_template('index.html')
        else:
            pas = db.session.query(UserList).filter(UserList.id==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.profile
                    if identy == 1:
                        error = None
                        if request.method == 'GET':
                            return render_template('index.html')
                        elif request.method == 'POST':
                            form = request.form
                            query = form.get('keyword', None)
                            level = form.get('level', None)
                            startstr = form.get('startdate', None)
                            endstr = form.get('enddate', None)

                            if query:
                                topic_id = acquire_topic_id(query)
                                if level == 'macroview':
                                    return render_template('macroview.html', keyword=query, startdate=startstr, enddate=endstr)
                                elif level == 'microview':
                                    return render_template('microview.html', keyword=query, startdate=startstr, enddate=endstr)
                                else:
                                    pass
                            else:
                                flash(u'请输入事件关键词！')
                                return render_template('index.html')
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')
    
@mod.route('/person_tab_ajax/<model>/<uid>')
def profile_person_tab_ajax(model, uid):
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if model == 'personaltopic':
                field_bucket = get_bucket('user_daily_field')
                try:
                    fields = field_bucket.Get(str(uid) + '_' + '20130430')
                    field1, field2 = fields.split(',')
                    field1 = unicode(fieldsEn2Zh(field1), 'utf-8')
                    field2 = unicode(fieldsEn2Zh(field2), 'utf-8')
                except KeyError:
                    field1 = 'unknown'
                    field2 = 'unknown'
                    field1 = unicode('未知', 'utf-8')
                    field2 = unicode('未知', 'utf-8')
                return render_template('profile/ajax/personal_word_cloud.html', uid=uid, fields=field1 + ',' + field2)
            elif model == 'personalweibocount':
                return render_template('profile/ajax/personal_weibo_count.html', uid=uid)
            elif model == 'personalnetwork':
                return render_template('profile/ajax/personal_network.html', uid=uid)
            elif model == 'personalnetwork_follow':       
                return render_template('profile/ajax/personalnetwork_follow.html', uid=uid)
            elif model == 'personalinteractnetwork':
                return render_template('profile/ajax/personal_friends_followers.html', uid=uid)
            elif model == 'grouptopic':
                return render_template('profile/ajax/group_word_cloud.html', field=uid)
            elif model == 'groupweibocount':
                return render_template('profile/ajax/group_weibo_count.html', field=uid)
            elif model == 'grouprank':
                return render_template('profile/ajax/group_rank_bloggers.html', field=uid)
            elif model == 'grouplocation':
                return render_template('profile/ajax/group_location.html', field=uid)
            elif model == 'groupactive':
                return render_template('profile/ajax/group_active.html', field=uid)
            elif model == 'groupemotion':
                return render_template('profile/ajax/group_emotion.html', field=uid)
        else:
            pas = db.session.query(UserList).filter(UserList.id==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.profile
                    if identy == 1:
                        if model == 'personaltopic':
                            field_bucket = get_bucket('user_daily_field')
                            try:
                                fields = field_bucket.Get(str(uid) + '_' + '20130430')
                                field1, field2 = fields.split(',')
                                field1 = unicode(fieldsEn2Zh(field1), 'utf-8')
                                field2 = unicode(fieldsEn2Zh(field2), 'utf-8')
                            except KeyError:
                                field1 = 'unknown'
                                field2 = 'unknown'
                                field1 = unicode('未知', 'utf-8')
                                field2 = unicode('未知', 'utf-8')
                            return render_template('profile/ajax/personal_word_cloud.html', uid=uid, fields=field1 + ',' + field2)
                        elif model == 'personalweibocount':
                            return render_template('profile/ajax/personal_weibo_count.html', uid=uid)
                        elif model == 'personalnetwork':
                            return render_template('profile/ajax/personal_network.html', uid=uid)
                        elif model == 'personalnetwork_follow':       
                            return render_template('profile/ajax/personalnetwork_follow.html', uid=uid)
                        elif model == 'personalinteractnetwork':
                            return render_template('profile/ajax/personal_friends_followers.html', uid=uid)
                        elif model == 'grouptopic':
                            return render_template('profile/ajax/group_word_cloud.html', field=uid)
                        elif model == 'groupweibocount':
                            return render_template('profile/ajax/group_weibo_count.html', field=uid)
                        elif model == 'grouprank':
                            return render_template('profile/ajax/group_rank_bloggers.html', field=uid)
                        elif model == 'grouplocation':
                            return render_template('profile/ajax/group_location.html', field=uid)
                        elif model == 'groupactive':
                            return render_template('profile/ajax/group_active.html', field=uid)
                        elif model == 'groupemotion':
                            return render_template('profile/ajax/group_emotion.html', field=uid)
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

def last_day(day_num=1):
    from datetime import date, timedelta
    now_date = date.today()
    last_date = now_date - timedelta(days=day_num)
    return last_date.isoformat(), now_date.isoformat()

def ts2hour(ts):
    return int(time.strftime('%H', time.localtime(ts)))
@mod.route('/person_hourly_pattern/<uid>', methods=['GET', 'POST'])
def profile_hourly_pattern(uid):
    if request.method == 'GET' and uid:
        results = []
        for duration in [182, 364, 728]:
            begin_str, end_str = last_day(duration)
            begin_ts = datetime2ts(begin_str)
            end_ts = datetime2ts(end_str)
            query_dict = {
                'user': uid,
                'timestamp': {'$gt': begin_ts, '$lt': end_ts}
            }
            hourly_count_dict = {}
            for hour_x in range(0, 24):
                hourly_count_dict[hour_x] = 0
            count, get_results = xapian_search_weibo.search(query=query_dict, fields=['timestamp'])
            for r in get_results():
                timestamp = r['timestamp']
                hournum = ts2hour(timestamp)
                hourly_count_dict[hournum] += 1
            sorted_hourly_count = sorted(hourly_count_dict.iteritems(), key=itemgetter(0), reverse=False)
            sorted_hourly_count = [[k, int(v * 100 / duration) / 100.0] for k, v in sorted_hourly_count]
            results.append(sorted_hourly_count)
        return json.dumps({'halfyear': results[0], 'oneyear': results[1], 'twoyear': results[2]})

@mod.route('/person_weekly_pattern/<uid>', methods=['GET', 'POST'])
def profile_weekly_pattern(uid):
    if request.method == 'GET' and uid:
        results = []
        for duration in [182, 364, 728]:
            begin_str, end_str = last_day(duration)
            begin_ts = datetime2ts(begin_str)
            end_ts = datetime2ts(end_str)
            query_dict = {
                'user': uid,
                'timestamp': {'$gt': begin_ts, '$lt': end_ts}
            }
            weekday_count_dict = {}
            for weekday_x in range(1, 8):
                weekday_count_dict[weekday_x] = 0
            count, get_results = xapian_search_weibo.search(query=query_dict, fields=['timestamp'])
            for r in get_results():
                timestamp = r['timestamp']
                weekday = date.fromtimestamp(timestamp).isoweekday()
                weekday_count_dict[weekday] += 1
            sorted_weekday_count = sorted(weekday_count_dict.iteritems(), key=itemgetter(0), reverse=False)
            sorted_weekday_count = [[k, int(v * 100 / (duration / 7)) / 100.0] for k, v in sorted_weekday_count]
            results.append(sorted_weekday_count)
        return json.dumps({'halfyear': results[0], 'oneyear': results[1], 'twoyear': results[2]})

@mod.route('/person_topic/<uid>', methods=['GET', 'POST'])
def profile_person_topic(uid):
    if request.method == 'GET' and uid:
        from datetime import datetime
        from ldamodel import lda_topic
        result_arr = []
        interval = None
        topic_limit = None
        keyword_limit = 100
        action = 'nonupdate'
        window_size = 24*60*60
        current_date = date.today().isoformat()
        if request.args.get('interval') and request.args.get('topic_limit') and request.args.get('keyword_limit') and request.args.get('action'):
            interval =  int(request.args.get('interval'))
            topic_limit =  int(request.args.get('topic_limit'))
            keyword_limit = int(request.args.get('keyword_limit'))
            action = request.args.get('action')
        lda_topic_bucket = get_bucket('user_lda_topics_20130808')
        if action == 'nonupdate':
            for topic_date in range(20130808, int(''.join(current_date.split('-'))) + 1):
                topic_key = str(uid) + '_' + str(topic_date) + '_' + str(window_size) + '_' + str(interval)
                lda_results = {}
                try:
                    lda_results = json.loads(lda_topic_bucket.Get(topic_key))
                    #sortedtopics = sorted(topics.iteritems(), key=operator.itemgetter(1), reverse=True)
                    #for k, v in sortedtopics[:limit]:
                    #    result_arr.append({'text': k, 'size': float(v)})
                    return json.dumps(lda_results)
                except KeyError:
                    continue
            topic_key = str(uid) + '_' + str(current_date) + '_' + str(window_size) + '_' + str(interval)
            startstr, endstr = last_day(interval)
            lda_results = lda_topic(uid, startstr, endstr)
            lda_topic_bucket.Put(topic_key, json.dumps(lda_results))
            return json.dumps(lda_results)
        return json.dumps(result_arr)
    else:
        return json.dumps([])

    
def social_graph(raw):
    graph_result = {'nodes': [], 'links': [{"source":0,"target":1,"value":20},{"source":0,"target":2,"value":16}]}
    center = raw['center']
    top_fri = raw['top_fri']
    top_fol = raw['top_fol']
    rest = raw['rest']
    graph_result['nodes'].append(social_node(center, 0, 0))
    node_idx = 0
    for uid in top_fri:
        weight = top_fri[uid]
        graph_result['nodes'].append(social_node(uid, 1, weight))
        node_idx += 1
        graph_result['links'].append({"source":0,"target":node_idx,"value":weight, 'group': 1})
    for uid in top_fol:
        weight = top_fol[uid]
        graph_result['nodes'].append(social_node(uid, 2, weight))
        node_idx += 1
        graph_result['links'].append({"source":0,"target":node_idx,"value":weight, 'group': 2})
    for uid in rest:
        weight = rest[uid]
        graph_result['nodes'].append(social_node(uid, 3, weight))
        node_idx += 1
        graph_result['links'].append({"source":0,"target":node_idx,"value":weight, 'group': 3})
    return graph_result

def social_node(user_id, group, weight):
    field_bucket = get_bucket('user_daily_field')
    try:
        fields = field_bucket.Get(str(user_id) + '_' + '20130430')
        field = fields.split(',')[0]
        field = unicode(fieldsEn2Zh(field), 'utf-8')
    except KeyError:
        field = ''
    count, get_results = xapian_search_user.search(query={'_id': int(user_id)}, fields=['profile_image_url', 'name'])
    name = None
    profile_image_url = None
    if count:
        for r in get_results():
            name = r['name']
            profile_image_url = r['profile_image_url']
    return {"name": name, "group":group, "profile_image_url": profile_image_url, "domain": field, "weight": weight}




def ts2date(ts):
    return date.fromtimestamp(int(float(ts)))
#new

@mod.route('/person_network/<friendship>/<uid>', methods=['GET', 'POST'])
def profile_network(friendship, uid):
    if request.method == 'GET':
        total_days = 89
        today = datetime.datetime.today()
        now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
        now_ts = int(now_ts)
        during = 24 * 3600

        if request.args.get('interval'):
            total_days =  int(request.args.get('interval')) - 1

        interact_bucket = get_bucket('user_daily_interact_count')
        friendship_bucket = get_bucket('friendship')
        fri_fol = []

        if friendship == 'friends':
            friends_key = str(uid) + '_' + 'friends'
            try:
                friends = json.loads(friendship_bucket.Get(friends_key))
            except KeyError:
                friends = []
            fri_fol = friends
        if friendship == 'followers':
            followers_key = str(uid) + '_' + 'followers'
            try:
                followers = json.loads(friendship_bucket.Get(followers_key))
            except KeyError:
                followers = []
            fri_fol = followers

        uid_interact_count = {} 
        for i in xrange(-total_days + 1, 1):
            lt = now_ts + during * i
            for f_uid in fri_fol:
                count = 0
                try:
                    count += int(interact_bucket.Get(str(uid) + '_' + str(f_uid) + '_' + str(lt)))
                except KeyError:
                    pass
                try:
                    count += int(interact_bucket.Get(str(f_uid) + '_' + str(uid) + '_' + str(lt)))
                except KeyError:
                    pass
                if count:
                    try:
                        uid_interact_count[str(f_uid)] += count
                    except KeyError:
                        uid_interact_count[str(f_uid)] = count

        sorted_counts = sorted(uid_interact_count.iteritems(), key=operator.itemgetter(1), reverse=True)
        try:
            results = sorted_counts[:10]
        except:
            results = sorted_counts[:(sorted_counts.length-1)]
        result = []
        for uid, count in results:
            user = getUserInfoById(uid)
            label = uid
            if user:
                label = user['userName']
            result.append([label, count])
        return json.dumps(result)

#new
@mod.route('/person_fri_fol/<friendship>/<uid>', methods=['GET', 'POST'])
def profile_person_fri_fol(friendship, uid):
    if request.method == 'GET' and uid and request.args.get('page'):
        page = int(request.args.get('page'))
        COUNT_PER_PAGE = 10
        if page == 1:
            startoffset = 0
        else:
            startoffset = (page - 1) * COUNT_PER_PAGE
        friendship_bucket = get_bucket('friendship')
        fri_fol = []
        if friendship == 'friends':
            friends_key = str(uid) + '_' + 'friends'
            try:
                friends = json.loads(friendship_bucket.Get(friends_key))
            except KeyError:
                friends = []
            fri_fol = friends
        if friendship == 'followers':
            followers_key = str(uid) + '_' + 'followers'
            try:
                followers = json.loads(friendship_bucket.Get(followers_key))
            except KeyError:
                followers = []
            fri_fol = followers
        field_bucket = get_bucket('user_daily_field')
        user_dict = {}
        field_user_count = {}
        if fri_fol != []:
            for user_id in set(fri_fol[:10000]):
                try:
                    fields = field_bucket.Get(str(user_id) + '_' + '20130430')
                    field = fields.split(',')[0]
                    field = unicode(fieldsEn2Zh(field), 'utf-8')
                except KeyError:
                    continue
                try:
                    field_user_count[field] = field_user_count[field] + 1
                except KeyError:
                    field_user_count[field] = 1
                count, get_results = xapian_search_user.search(query={'_id': user_id}, fields=['_id', 'name', 'statuses_count', 'followers_count', 'friends_count'])
                if count > 0:
                    for user in get_results():
                        user_dict[user_id] = [user['name'], user['statuses_count'], user['followers_count'], user['friends_count'], field]
        sorted_users = sorted(user_dict.items(), key=lambda d: d[1][2], reverse=True)
        result = []
        for id, value in sorted_users:
            result.append({'id': id,'userName': unicode(value[0], "utf-8"), 'statusesCount': value[1],
                           'followersCount': value[2], 'friendsCount': value[3],
                           'field': value[4]})
        total_pages = len(result) / COUNT_PER_PAGE + 1
        try:
            users = result[startoffset:(startoffset+COUNT_PER_PAGE-1)]
        except:
            users = result[startoffset: len(result)-1]
        sorted_field_count = sorted(field_user_count.items(), key=lambda d: d[1], reverse=True)
        return json.dumps({'users': users, 'pages': total_pages, 'fields': sorted_field_count})

seed_set = set([])
with open('weibo/profile/data/official_emoticons.txt') as f:
    for l in f:
        seed_set.add(l.rstrip())
def emoticon_find(seed_set, text):
    emotion_pattern = r'\[(\S+?)\]'
    remotions = re.findall(emotion_pattern, text)

    emoticons = []
    if remotions:
        for e in remotions:
            if e in seed_set:
                emoticons.append(e.decode('utf-8'))

    return emoticons

def getTopWbs(uid, mid):
    url = weiboinfo2url(uid, mid)
    query_dict = {
        '_id': int(mid),
    }
    count, get_results = xapian_search_weibo.search(query=query_dict, fields=['timestamp', 'reposts_count', 'comments_count', 'attitudes_count', 'text', 'source'])
    
    for r in get_results():
        created_at = ts2HMS(r['timestamp'])
        reposts_count = r['reposts_count']
        comments_count = r['comments_count']
        attitudes_count = r['attitudes_count']
        text = r['text']
        source = r['source']
        return [created_at, url, reposts_count, comments_count, attitudes_count, text, source]
#new

@mod.route('/person_count/<uid>', methods=['GET', 'POST'])
def personal_weibo_count(uid):
    post_status_kv = {'total': 2, 'repost': 1, 'fipost': 0}

    total_days = 89
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600
    time_arr = []
    post_arr = []
    repost_arr = []
    fipost_arr = []

    if request.args.get('interval'):
        total_days =  int(request.args.get('interval')) - 1

    begin_ts = now_ts + during * (-total_days + 1)
    end_ts = now_ts
    query_dict = {
        'user': uid,
        'timestamp': {'$gt': begin_ts, '$lt': end_ts}
    }
    count, get_results = xapian_search_weibo.search(query=query_dict, fields=['timestamp', 'retweeted_status', '_id', 'reposts_count', 'comments_count', 'text'])
    daily_count_dict = {}
    comments_count_dict = {}
    reposts_count_dict = {}
    total_post_count = count
    retweets_count = 0
    emoticons_count = 0
    for r in get_results():
        emoticons = emoticon_find(seed_set, r['text'])
        if emoticons and len(emoticons):
            emoticons_count += 1
        datestr = date.fromtimestamp(r['timestamp']).isoformat()
        try:
            daily_count_arr = daily_count_dict[datestr]
        except KeyError:
            daily_count_dict[datestr] = [0, 0, 0, r['timestamp']]#原创，转发，总数，时间戳
        if r['retweeted_status']:
            daily_count_dict[datestr][1] += 1
            retweets_count += 1
        else:
            daily_count_dict[datestr][0] += 1
        daily_count_dict[datestr][2] += 1
        comments_count_dict[r['_id']] = r['comments_count']
        reposts_count_dict[r['_id']] = r['reposts_count']
    sorted_daily_count = sorted(daily_count_dict.iteritems(), key=lambda(k, v): v[3])
    sorted_reposts_count = sorted(reposts_count_dict.iteritems(), key=itemgetter(1), reverse=True)
    sorted_comments_count = sorted(comments_count_dict.iteritems(), key=itemgetter(1), reverse=True)
    for k, v in sorted_daily_count:
        time_arr.append(k)
        fipost_arr.append(v[0])
        repost_arr.append(v[1])
        post_arr.append(v[2])
    total_reposts_count = sum(reposts_count_dict.values())
    avg_reposts_count = total_reposts_count / count
    total_comments_count = sum(comments_count_dict.values())
    avg_comments_count = total_comments_count / count
    top_3_reposts = sorted_reposts_count[:3]
    top_3_comments = sorted_comments_count[:3]
    top_3_reposts_wbs = [getTopWbs(uid, mid) for mid, count in top_3_reposts]
    top_3_comments_wbs = [getTopWbs(uid, mid) for mid, count in top_3_comments]

    return json.dumps({'time': time_arr, 'count': post_arr, 'repost': repost_arr, 'fipost': fipost_arr, \
        'total_reposts_count': total_reposts_count, 'total_comments_count': total_comments_count, \
        'avg_reposts_count': avg_reposts_count, 'avg_comments_count': avg_comments_count, \
        'top_3_reposts_wbs': top_3_reposts_wbs, 'top_3_comments_wbs': top_3_comments_wbs, \
        'total_tweets': total_post_count, 'retweets_ratio': int(retweets_count * 100 / total_post_count) / 100.0, \
        'emoticons_ratio': int(emoticons_count * 100 / total_post_count) / 100.0})

def getUserInfoByName(name):
    count, get_results = xapian_search_user.search(query={'name': name}, fields=['profile_image_url', '_id', 'friends_count', \
                                          'statuses_count', 'followers_count', 'gender', 'verified', 'created_at', 'location'])
    if count:
        for r in get_results():
            user = {'id': r['_id'], 'profile_image_url': r['profile_image_url'], 'userName':  name, 'friends_count': r['friends_count'], \
                    'statuses_count': r['statuses_count'], 'followers_count': r['followers_count'], 'gender': r['gender'], \
                    'verified': r['verified'], 'created_at': r['created_at'], 'location': unicode(r['location'], "utf-8")}
            return user
    else:
        return 'no such user'
def getUserInfoById(uid):
    count, get_results = xapian_search_user.search(query={'_id': uid}, fields=['profile_image_url', 'name', 'friends_count', \
                                          'statuses_count', 'followers_count', 'gender', 'verified', 'created_at', 'location'])
    if count:
        for r in get_results():
            user = {'id': uid, 'profile_image_url': r['profile_image_url'], 'userName':  r['name'], 'friends_count': r['friends_count'], \
                    'statuses_count': r['statuses_count'], 'followers_count': r['followers_count'], 'gender': r['gender'], \
                    'verified': r['verified'], 'created_at': r['created_at'], 'location': unicode(r['location'], "utf-8")}
            return user
    else:
        return None

@mod.route('/topic/macroview/<topic>/', methods=['POST', 'GET'])
def macroview(topic):
    if request.method == 'GET':
        return render_template('macroview.html', keyword=topic)
    else:
        return None

@mod.route('/summary/')
def summary():
    query = request.args.get('query', '')
    query = query.strip()
    start_date='2013-1-1'
    end_date='2013-6-1'
    start_ts = time2ts(start_date)
    end_ts = time2ts(end_date)

    uidset = set()
    mints = 0
    maxts = 0
    total_count = 0
    reposts_count = 0
    if query:
        query_dict = {
            'timestamp': {'$gt': start_ts, '$lt': end_ts},
            '$or': []
        }
        for term in query.split(','):
            if term:
                query_dict['$or'].append({'text': [term]})
        count, get_results = xapian_search_weibo.search(query=query_dict, fields=['user', 'retweeted_status', 'text'])
        for r in get_results():
            total_count += 1
            try:
                ts = r['timestamp']
            except KeyError:
                ts = 0
            if mints > ts:
                mints = ts
            if maxts < ts:
                maxts = ts
            uidset.add(r['user'])
            retweeted_mid = r['retweeted_status']
            if retweeted_mid:
                reposts_count += 1
                retweeted_uid = search_user_by_mid(retweeted_mid)
            else:
                retweeted_uid = None
            if retweeted_uid:
                uidset.add(retweeted_uid)
            text = r['text']
            if text:
                repost_chain = re.findall(r'//@(\S+?):', text)
                if repost_chain:
                    for username in repost_chain:
                        uidset.add(search_uid_by_username(username))
            
        print len(uidset), mints, maxts, total_count, reposts_count
        return json.dumps([len(uidset), mints, maxts, total_count, reposts_count])

@mod.route('/time_pattern/')
def time_pattern():
    """获取特定事件的时间模式
    """

    query = request.args.get('query', '')
    query = query.strip()
    ts = request.args.get('ts', '')
    ts = long(ts)
    during = 24 * 3600

    results_dict = {}
    if query:
        begin_ts = ts - during
        end_ts = ts
        repost_query_dict = {
            'timestamp': {'$gt': begin_ts, '$lt': end_ts},
            '$or': [],
            '$not': {'retweeted_status': '0'}
        }
        fipost_query_dict = {
            'timestamp': {'$gt': begin_ts, '$lt': end_ts},
            '$or': [],
            'retweeted_status': '0'
        }
        total_query_dict = {
            'timestamp': {'$gt': begin_ts, '$lt': end_ts},
            '$or': []
        }
        text_query_list = []
        for term in query.split(','):
            if term:
                text_query_list.append({'text': [term]})
        '''
        uid_query_list = []
        uid_20s = getSampleUsers()['uids']
        for uid in uid_20s:
            uid_query_list.append({'user': uid})
        repost_query_dict['$or'] = text_query_list + uid_query_list 
        fipost_query_dict['$or'] = text_query_list + uid_query_list
        total_query_dict['$or'] = text_query_list + uid_query_list
        '''
        repost_query_dict['$or'] = text_query_list
        fipost_query_dict['$or'] = text_query_list
        total_query_dict['$or'] = text_query_list
        repost_count = xapian_search_weibo.search(query=repost_query_dict, count_only=True)
        fipost_count = xapian_search_weibo.search(query=fipost_query_dict, count_only=True)
        total_count = xapian_search_weibo.search(query=total_query_dict, count_only=True)
        print repost_count, fipost_count, total_count
        return json.dumps({'count': [fipost_count, repost_count, total_count]})
    else:
        return json.dumps([])

@mod.route('/spatial_pattern/')
def spatial_pattern():
    """获取特定事件的空间模式
    """
    query = request.args.get('query', '')
    query = query.strip()
    ts = request.args.get('ts', '')
    ts = long(ts)
    during = 24 * 3600

    city_count = {}
    if query:
        begin_ts = ts - during
        end_ts = ts
        query_dict = {
            'timestamp': {'$gt': begin_ts, '$lt': end_ts},
            '$or': []
        }
        for term in query.split(','):
            if term:
                query_dict['$or'].append({'text': [term]})
        count, get_results = xapian_search_weibo.search(query=query_dict, fields=['user', 'retweeted_status'])
        for r in get_results():
            user = r['user']
            retweeted_status = r['retweeted_status']
            if user:
                u_co, u_re = xapian_search_user.search(query={'_id': int(user)}, fields=['location'])
                if u_co:
                    for u_r in u_re():
                        location = u_r['location']
                        province = getProvince(location)
                        if province != -1:
                            try:
                                status = city_count[province]
                            except KeyError:
                                status = [[0, 0], 0]#原创数，转发数，微博总数
                            if retweeted_status == None:
                                status[0][0] += 1
                            else:
                                status[0][1] += 1
                            status[1] += 1
                            city_count[province] = status
        results = province_color_map(city_count)
    return json.dumps(results)

@mod.route('/emotiondata/')
def emotion_data():
    """获取特定事件的分类情感数据
    """
    query = request.args.get('query', '')
    query = query.strip()
    ts = request.args.get('ts', '')
    ts = long(ts)
    during = 24 * 3600

    emotions_data = {}
    results = {}
    if query:
        begin_ts = ts - during
        end_ts = ts
        query_dict = {
            'timestamp': {'$gt': begin_ts, '$lt': end_ts},
            '$or': []
        }
        for term in query.split(','):
            if term:
                query_dict['$or'].append({'text': [term]})
        for k, v in emotions_kv.iteritems():
            query_dict['sentiment'] = v
            count = xapian_search_sentiment.search(query=query_dict, count_only=True)
            emotions_data[k] = count
        sum_count = sum(emotions_data.values())
        if sum_count > 0:
            results['count'] = [int(emotions_data['happy'] * 100/ sum_count) / 100.0, int(emotions_data['angry'] * 100/ sum_count) / 100.0, int(emotions_data['sad'] * 100/ sum_count) / 100.0]
            results['date'] = date.fromtimestamp(end_ts).isoformat()
            results['event'] = []
        else:
            results = 'None'
    return json.dumps(results)

@mod.route('/emotion_weibos_data/<area>/')
def weibos_data(area='global'):
    query = request.args.get('query', '')
    query = query.strip()
    ts = request.args.get('ts', '')
    ts = long(ts)

    during = 3600

    begin_ts = ts - during
    end_ts = ts
    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
        'reposts_count': {'$gt': 1000},
    }
    if query:
        query_dict['$or'] = []
        for term in query.split(','):
            if term:
                query_dict['$or'].append({'text': term})

    count, get_results = xapian_search_weibo.search(query=query_dict, max_offset=5, sort_by=['-reposts_count'], fields=['text', 'timestamp'])
    print count
    data = list(get_results())

    return json.dumps(data)

import numpy as np
def st_variation(lis1, lis2, lis3):
    ave = np.mean(lis1)    
    variation1 = [np.abs(num - ave)/ave for num in lis1]
    ave = np.mean(lis2)
    variation2 = [np.abs(num - ave)/ave for num in lis2]
    ave = np.mean(lis3)
    variation3 = [np.abs(num - ave)/ave for num in lis3]
    variation = [variation1[cursor]+variation2[cursor]+variation3[cursor] for cursor in range(len(lis1))]
    return variation

def find_topN(lis,n):
    new = [lis[0]]
    rank = [0]
    num_cursor = 1
    for num in lis[1:]:
        num_cursor += 1
        find = 0
        cursor = 0
        if num > new[0]:
            new[0:0] = [num]
            rank[0:0] = [num_cursor-1]
        else:
            for i in new:
                if num > i:
                    new[cursor:cursor] = [num]
                    rank[cursor:cursor] = [num_cursor-1]
                    find = 1
                    break
                cursor += 1
            if find == 0:
                new.append(num)
                rank.append(num_cursor-1)
            
    peak_x = []
    peak_y = []
    cursor = 0
    for y in new:
        if rank[cursor]!=0 and rank[cursor]!=len(new)-1:
            if y > lis[rank[cursor]+1] and y > lis[rank[cursor]-1]:
                peak_x.append(rank[cursor])
                peak_y.append(y)

        elif rank[cursor]==0:
            if y > lis[rank[cursor]+1]:
                peak_x.append(rank[cursor])
                peak_y.append(y)
        elif rank[cursor]==rank[cursor]!=len(new)-1:
            if y > lis[rank[cursor]+1]:
                peak_x.append(rank[cursor])
                peak_y.append(y)
        if len(peak_x)==n:
            break
        cursor += 1
    return peak_x[:n],peak_y[:n]

@mod.route('/emotionpeak/')
def getPeaks():
    happy_lis = request.args.get('happy', '')
    angry_lis = request.args.get('happy', '')
    sad_lis = request.args.get('sad', '')
    ts_lis = request.args.get('ts', '')
    query = request.args.get('query', '')
    query = query.strip()

    happy_lis = [float(da) for da in happy_lis.split(',')]
    angry_lis = [float(da) for da in angry_lis.split(',')]
    sad_lis = [float(da) for da in sad_lis.split(',')]
    ts_lis = [float(da) for da in ts_lis.split(',')]
    topN = 10
    sentiment_variation = st_variation(happy_lis, angry_lis, sad_lis)
    ##peak_x返回前N个点的在list中的序数0,1.
    ##peak_y返回前N个点的情绪波动值
    peak_x,peak_y = find_topN(sentiment_variation,topN)
    time_lis = {}
    for i in peak_x:
        during = 24 * 3600
        ts = ts_lis[i]
        begin_ts = ts - during
        end_ts = ts
        ann_data = {}
        cloud_text = {'happy': [], 'angry': [], 'sad': []}
        for emotion in emotions_kv.keys():
            query_dict = {
                'timestamp': {'$gt': begin_ts, '$lt': end_ts},
                'sentiment': emotions_kv[emotion],
                '$or': []
            }
            for term in query.split(','):
                if term:
                    query_dict['$or'].append({'text': [term]})
            count, get_results = xapian_search_sentiment.search(query=query_dict, fields=['terms', 'text', 'user'])
            keywords_with_50count = top_keywords(get_results, top=50)
            keywords_with_10count = top_keywords(get_results, top=10)
            ann_text = ','.join([tp[0] for tp in keywords_with_10count])
            for kw, c in keywords_with_50count:
                cloud_text[emotion].append({'w': kw, 'c': c})
            '''
            ann_text_list = []
            limit_count = 0
            for r in get_results():
                if limit_count > 3:
                    break
                print r['user'], r['text']
                ann_text_list.append('用户<a target="_blank" href="http://weibo.com/u/' + r['user'] + '/>' + r['user'] +'</a>"说：' + r['text'])
                limit_count += 1
            '''
            ann_data[emotion] = {
                'title': emotion,
                'text':  ann_text
            }
        time_lis[i] = {'ts': ts_lis[i], 'ann_data': ann_data, 'cloud_data': cloud_text}
    return json.dumps(time_lis)
#new

@mod.route('/group_topic/<fieldEnName>')
def profile_group_topic(fieldEnName):
    if request.method == 'GET' and fieldEnName:
        result_arr = []
        interval = None
        sort = None
        topic_type = None
        limit = 100
        window_size = 24*60*60
        if request.args.get('interval') and request.args.get('sort') and request.args.get('limit') and request.args.get('topic_type'):
            interval =  int(request.args.get('interval'))
            sort =  request.args.get('sort')
            limit = int(request.args.get('limit'))
            topic_type = request.args.get('topic_type')
        if topic_type == 'lda':
            field_lda_bucket = get_bucket('field_lda_topics')
            try:
                topics = json.loads(field_lda_bucket.Get(str(fieldEnName)))
                sortedtopics = sorted(topics.iteritems(), key=operator.itemgetter(1), reverse=True)
                for k, v in sortedtopics[:limit]:
                    result_arr.append({'text': k, 'size': float(v)})
            except KeyError:
                result_arr = []
        if topic_type == 'bst':
            from burst_word import read_field_burst_wordsFromDb, burst_model_field, sort_busrt_words
            from utils import last_day
            time_start, time_end = last_day(interval)
            if interval == 1:
                window_size = 3600
            else:
                window_size = 24*60*60
            topics = read_field_burst_wordsFromDb(fieldEnName, time_start, time_end, window_size)
            if topics:
                result_arr = sort_busrt_words(topics, sort=sort, limit=limit)
            else:
                new_topics = burst_model_field(time_start, time_end, field=fieldEnName, window_size=window_size)
                if new_topics:
                    result_arr = sort_busrt_words(new_topics, sort=sort, limit=limit)
        return json.dumps(result_arr)
    else:
        return json.dumps([])

    return json.dumps(result_arr)

@mod.route('/group_count/<fieldEnName>', methods=['GET', 'POST'])
def group_status_count(fieldEnName):
    total_days = 89
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600

    time_arr = []
    total_arr = []
    repost_arr = []
    fipost_arr = []

    interval = None
    if request.args.get('interval'):
        interval =  int(request.args.get('interval'))
        total_days = interval

    bucket = get_bucket('field_daily_post_count')
    for i in xrange(-total_days + 1, 1):
        lt = now_ts + during * i
        post_count = {}
        for is_retweeted in [0, 1]:        
            try:
                daily_count = bucket.Get(str(fieldEnName) + '_' + str(lt) + '_' + str(is_retweeted))
                daily_count = int(daily_count)
            except KeyError:
                daily_count = 0
            post_count[is_retweeted] = daily_count
        sumcount = sum(post_count.values())
        if sumcount > 0:
            time_arr.append(ts2date(lt).isoformat())
            total_arr.append(sumcount)
            repost_arr.append(post_count[1])
            fipost_arr.append(post_count[0])
        
    return json.dumps({'time': time_arr, 'count': total_arr, 'repost': repost_arr, 'fipost': fipost_arr})

@mod.route('/group_active/<fieldEnName>', methods=['GET', 'POST'])
def group_active_count(fieldEnName):
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600

    interval = 6
    if request.args.get('interval'):
        interval =  int(request.args.get('interval'))

    bucket = get_bucket('field_daily_active_count')
    uids_set = set([k.split('_')[2] for k in bucket.RangeIter(include_value = False)])
    user_count = {}
    for i in xrange(-interval + 1, 1):
        lt = now_ts + during * i
        for uid in uids_set:    
            try:
                daily_count = bucket.Get(str(fieldEnName) + '_' + str(lt) + '_' + str(uid))
                daily_count = int(daily_count)
            except KeyError:
                daily_count = 0
            if daily_count > 0:
                try:
                    user_count[uid] += daily_count
                except KeyError:
                    user_count[uid] = daily_count

    post_list = [count for user, count in user_count.items()]
    freq = [post_list.count(u) for u in post_list]
    count_dict = dict(zip(post_list,freq))
    result = sorted(count_dict.iteritems(), key=operator.itemgetter(0), reverse=False)

    trunc_n = 20
    x_list = []
    y_list = []

    if len(result) > trunc_n:
        trunc_after_sum = sum([v[1] for i, v in enumerate(result) if i >= trunc_n])
        for n in xrange(0, trunc_n):
            if n == trunc_n - 1:
                x_list.append(str(result[n][0]) + '+')
                y_list.append(trunc_after_sum)
            else:
                x_list.append(result[n][0])
                y_list.append(result[n][1])
    else:    
        for count, freq in result:
            x_list.append(count)
            y_list.append(freq)
    return json.dumps({'x': x_list, 'y': y_list})

@mod.route('/group_emotion/<fieldEnName>')
def profile_group_emotion(fieldEnName):
    total_days = 89
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600

    time_arr = []
    happy_arr = []
    angry_arr = []
    sad_arr = []

    interval = None
    if request.args.get('interval'):
        interval =  int(request.args.get('interval'))
        total_days = interval

    bucket = get_bucket('field_daily_sentiment_count')
    for i in xrange(-total_days + 1, 1):
        lt = now_ts + during * i
        emotion_count = {}
        for emotion in emotions_kv.values():        
            try:
                daily_emotion_count = bucket.Get(str(fieldEnName) + '_' + str(lt) + '_' + str(emotion))
                daily_emotion_count = int(daily_emotion_count)
            except KeyError:
                daily_emotion_count = 0
            emotion_count[emotion] = daily_emotion_count
        if sum(emotion_count.values()) > 0:
            sumcount = sum(emotion_count.values())
            time_arr.append(ts2date(lt).isoformat())
            happy_arr.append(int(emotion_count[1] * 100 / sumcount) / 100.0)
            angry_arr.append(int(emotion_count[2] * 100 / sumcount) / 100.0)
            sad_arr.append(int(emotion_count[3] * 100 / sumcount) / 100.0)

    return json.dumps({'time': time_arr, 'happy': happy_arr, 'angry': angry_arr, 'sad': sad_arr})

@mod.route('/group_verify/<fieldEnName>')
def profile_group_verify(fieldEnName):
    update_date = '20130430'
    bucket = get_bucket('field_daily_verify_count')
    try:
        verified_count = int(bucket.Get(str(fieldEnName) + '_' + update_date + '_' + 'True'))
    except KeyError:
        verified_count = 0
    try:
        normal_count = int(bucket.Get(str(fieldEnName) + '_' + update_date + '_' + 'False'))
    except KeyError:
        normal_count = 0

    result_list = ''
    if sum([verified_count, normal_count]) > 0:
        sumcount = sum([verified_count, normal_count])
        result_list = str(verified_count) + ',' + str(normal_count) + ',' + str(int(verified_count * 100 / sumcount) / 100.00) + ',' +  str(1 - int(verified_count * 100 / sumcount) / 100.00)

    return json.dumps(result_list)

@mod.route('/group_rank/<fieldEnName>')
def profile_group_rank(fieldEnName):
    page = 1
    countperpage = 8
    limit = 1000
    if request.args.get('page'):
        page = int(request.args.get('page'))
    if request.args.get('countperpage'):
        countperpage = int(request.args.get('countperpage'))
    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
    if page == 1:
        startoffset = 0
    else:
        startoffset = (page - 1) * countperpage
    endoffset = startoffset + countperpage - 1
    uids = getFieldUsersByScores(fieldEnName, startoffset, endoffset)
    users = []
    for uid in uids:
        user = getUserInfoById(uid)
        if user:
            users.append(user)
    total_pages = limit / countperpage + 1
    return json.dumps({'users': users, 'pages': total_pages})

@mod.route('/group_location/<fieldEnName>')
def profile_group_location(fieldEnName):
    from city_color import province_color_map
    city_count = {}
    loc_count_bucket = get_bucket('field_daily_location_count')
    pro_city_bucket = get_bucket('province_city')
    update_date = '20130430'
    provinceid_set = set([k.split('_')[1] for k in pro_city_bucket.RangeIter(include_value = False) if k.split('_')[0] == 'provinceid'])
    for pro_id in provinceid_set:
        try:
            count = loc_count_bucket.Get(fieldEnName + '_' + update_date + '_' + str(pro_id))
            pro_str = pro_city_bucket.Get('provinceid_' + str(pro_id))
            city_count[pro_str] = int(count)
        except KeyError:
            continue
    return json.dumps(province_color_map(city_count))

@mod.route('/result.json')
def graph_result():
    return json.dumps(graph_result)
graph_result = {"nodes":[{"name":"薛蛮子","group":0,"profile_image_url":"0","domain":"社会"},                         
                         {"name":"内分泌顾锋","group":1,"profile_image_url":"1","domain":"报刊"},
                         {"name":"留几手","group":1,"profile_image_url":"2","domain":"IT"},
                         {"name":"妈咪Jane育儿妙方","group":1,"profile_image_url":"3","domain":"传媒"},
                         {"name":"好友美食","group":2,"profile_image_url":"4","domain":"社会"},
                         {"name":"毛高山","group":2,"profile_image_url":"5","domain":"财经"},
                         {"name":"管鹏","group":2,"profile_image_url":"6","domain":"社会"},
                         {"name":"协和章蓉娅","group":3,"profile_image_url":"7","domain":"教育"},
                         {"name":"张遇升","group":3,"profile_image_url":"8","domain":"社会"},
                         {"name":"炎黄春秋编辑部","group":3,"profile_image_url":"9","domain":"社会"},
                         {"name":"苏家桥","group":4,"profile_image_url":"10","domain":"社会"},
                         {"name":"精神科李医生","group":4,"profile_image_url":"11","domain":"社会"},
                         {"name":"最活跃的关注者","group":-1,"profile_image_url":"1-1","domain":""},
                         {"name":"联系最紧密的关注者","group":-2,"profile_image_url":"1-1","domain":""},
                         {"name":"最活跃的粉丝","group":-3,"profile_image_url":"1-1","domain":""},
                         {"name":"联系最紧密的粉丝","group":-4,"profile_image_url":"1-1","domain":""},
                         {"name":"释源祖庭白马寺","group":5,"profile_image_url":"12","domain":""},
                         {"name":"Brad_Pitt","group":5,"profile_image_url":"13","domain":""},
                         {"name":"胡泽涛-Henry","group":5,"profile_image_url":"14","domain":""},
                         {"name":"姚树坤","group":5,"profile_image_url":"15","domain":""},
                         {"name":"瞬间就笑岔气了","group":5,"profile_image_url":"16","domain":""},
                         {"name":"口袋悦读","group":5,"profile_image_url":"17","domain":""},
                         {"name":"慕容雪村","group":5,"profile_image_url":"18","domain":""},
                         {"name":"老树画画","group":5,"profile_image_url":"19","domain":""},
                         {"name":"大河网信阳频道吴彦飞","group":5,"profile_image_url":"20","domain":""},
                         {"name":"桔子水晶吴海","group":5,"profile_image_url":"21","domain":""},
                         {"name":"谢维冰","group":5,"profile_image_url":"22","domain":""},
                         {"name":"社科院杨团","group":5,"profile_image_url":"23","domain":""},
                         {"name":"围脖唯美句","group":5,"profile_image_url":"24","domain":""},
                         {"name":"参考消息","group":5,"profile_image_url":"25","domain":""},
                         {"name":"九个头条","group":5,"profile_image_url":"26","domain":""},
                         {"name":"天才小熊猫","group":5,"profile_image_url":"27","domain":""},
                         {"name":"赵克罗","group":5,"profile_image_url":"28","domain":""},
                         {"name":"泰国苏梅岛自助游指南","group":5,"profile_image_url":"29","domain":""},
                         {"name":"苏宁孙为民","group":5,"profile_image_url":"30","domain":""},
                         {"name":"马翾","group":5,"profile_image_url":"31","domain":""}],
"links":[{"source":0,"target":12,"value":20},{"source":0,"target":13,"value":16},{"source":0,"target":14,"value":18},{"source":0,"target":15,"value":18},
         {"source":12,"target":1,"value":2},{"source":12,"target":2,"value":8},{"source":12,"target":3,"value":10},{"source":13,"target":4,"value":6},
         {"source":13,"target":5,"value":2},{"source":13,"target":6,"value":8},{"source":14,"target":7,"value":10},{"source":14,"target":8,"value":6},
         {"source":14,"target":9,"value":2},{"source":15,"target":10,"value":8},{"source":15,"target":11,"value":10},{"source":0,"target":16,"value":0.5},
         {"source":0,"target":17,"value":0.5},{"source":0,"target":18,"value":0.5},{"source":0,"target":19,"value":0.5},{"source":0,"target":20,"value":0.5},
         {"source":0,"target":21,"value":0.5},{"source":0,"target":22,"value":0.5},{"source":0,"target":23,"value":0.5},{"source":0,"target":24,"value":0.5},
         {"source":0,"target":25,"value":0.5},{"source":0,"target":26,"value":0.5},{"source":0,"target":27,"value":0.5},{"source":0,"target":28,"value":0.5},
         {"source":0,"target":29,"value":0.5},{"source":0,"target":30,"value":0.5},{"source":0,"target":31,"value":0.5},{"source":0,"target":32,"value":0.5},
         {"source":0,"target":33,"value":0.5},{"source":0,"target":34,"value":0.5},{"source":0,"target":35,"value":0.5}]}
