# -*- coding: utf-8 -*-

try:
    import simplejosn as json
except ImportError:
    import json
import os
import re
import sys
import time
import json
import urllib2
import operator
import datetime
from operator import itemgetter
from flask.ext import admin
from flask import Flask, url_for, render_template, request, make_response, flash, abort, Blueprint, session, redirect
from city_color import province_color_map
from utils import acquire_topic_id, read_rank_results, pagerank_rank, degree_rank, make_network_graph, get_above100_weibos, weiboinfo2url, emoticon_find, ts2hour
from datetime import date
from utils import ts2date, getFieldUsersByScores, datetime2ts, last_day, ts2datetime, ts2HMS
from xapian_weibo.xapian_backend import Schema, XapianSearch
from ldamodel import lda_topic
from weibo.extensions import db
from weibo.model import *
from flask.ext.sqlalchemy import Pagination
import leveldb
from utils import last_day

xapian_search_user = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
xapian_search_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
xapian_search_sentiment = XapianSearch(path='/opt/xapian_weibo/data/20130807', name='master_timeline_sentiment', schema_version=3)
xapian_search_domain  = XapianSearch(path='/home/xapian/var/lib/xapian_weibo/data/liumengni', name='master_timeline_domain', schema_version=4)

LEVELDBPATH = '/home/mirage/leveldb'
buckets = {}
emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
emotions_zh_kv = {'happy': '高兴', 'angry': '愤怒', 'sad': '悲伤'}
fields_value = [{'fieldZhName': u'文化', 'fieldEnName': 'culture'}, {'fieldZhName': u'教育', 'fieldEnName': 'education'}, {'fieldZhName': u'娱乐', 'fieldEnName': 'entertainment'}, {'fieldZhName': u'时尚', 'fieldEnName': 'fashion'}, {'fieldZhName': u'财经', 'fieldEnName': 'finance'}, {'fieldZhName': u'媒体', 'fieldEnName': 'media'}, {'fieldZhName': u'体育', 'fieldEnName': 'sports'}, {'fieldZhName': u'技术', 'fieldEnName': 'technology'}]
fields_id = {'culture':1, 'education':2, 'entertainment':3, 'fashion':4, 'finance':5, 'media':6, 'sports':7, 'technology':8}

mod = Blueprint('profile', __name__, url_prefix='/profile')
COUNT_PER_PAGE = 20

statusbound_list = []

def get_bucket(bucket):
    if bucket in buckets:
        return buckets[bucket]
    buckets[bucket] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_' + bucket), block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return buckets[bucket]


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

def getStaticInfo():
    statuscount = [0, 2000000, 4000000, 6000000, 8000000, 10000000, 12000000, 14000000, 16000000, 18000000, 20000000]
    friendscount = [0, 400, 800, 1200, 1600, 2000, 2400, 2800, 3200, 3600, 4000]
    followerscount = [0, 6000000, 12000000, 18000000, 24000000, 30000000, 36000000, 42000000, 48000000, 54000000, 60000000]
    province = ['北京',  '上海', '香港', '台湾', '重庆', '澳门', '天津', '江苏', '浙江', '四川', '江西', '福建', '青海', '吉林', '贵州', '陕西', '山西', '河北', '湖北', '辽宁', '湖南', '山东', '云南', '河南', '广东', '安徽', '甘肃', '海南', '黑龙江', '内蒙古', '新疆', '广西', '宁夏', '西藏', '海外']
    statuscount = db.session.query(RangeCount).filter(RangeCount.countType=='statuses').all()
    friendscount = db.session.query(RangeCount).filter(RangeCount.countType=='friends').all()
    followerscount = db.session.query(RangeCount).filter(RangeCount.countType=='followers').all()
    province = db.session.query(Province).order_by(Province.id).all()
    return statuscount, friendscount, followerscount, province, fields_value


def yymInfo(uid):
    query_dict = {
        '_id': int(uid)
    }
    user = xapian_search_user.search_by_id(int(uid), fields=['created_at', '_id', 'name', \
        'statuses_count', 'followers_count', 'friends_count', 'description', 'profile_image_url', 'verified', 'gender'])
    if user:
        r = user
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
    else:
        return None 

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
                        count, get_results = xapian_search_user.search(query={'_id':int(uid)}, fields=['created_at', '_id', 'name', 'statuses_count', 'followers_count', 'friends_count', 'description', 'profile_image_url'] )
                        for r in get_results():
                            statuses_count = r['statuses_count']
                            followers_count = r['followers_count']
                            friends_count = r['friends_count']
                            userName = r['name']
                            description = r['description']
                            uid = r['_id']
                            profile_image_url = r['profile_image_url']
                            user = {'id': uid, 'userName': userName, 'statusesCount': statuses_count, 'followersCount': followers_count, 'friendsCount': friends_count,
                                      'description': description, 'profileImageUrl': profile_image_url}
                            if user:
                                users.append(user)
                    return json.dumps(users[startoffset:endoffset])
                elif model in fields_value:
                    page = int(request.form['page'])
                    if page == 1:
                        startoffset = 0
                    else:
                        startoffset = (page - 1) * COUNT_PER_PAGE
                    endoffset = startoffset + COUNT_PER_PAGE - 1
                    fieldEnName = model
                    count, field_users = xapian_search_domain.search(query={'domain':str(fields_id[str(fieldEnName)])}, sort_by=['followers_count'], fields=['_id'], max_offset=10000)
                    field_users_list = [user['_id'] for user in field_users()]
                    users = []
                    for uid in field_users_list[startoffset: endoffset]:
                        user = xapian_search_user.search_by_id(int(uid), fields=['name', 'statuses_count', 'friends_count', 'followers_count', 'profile_image_url', 'description'])
                        if user:
                            users.append({'id': int(uid), 'profileImageUrl': user['profile_image_url'], 'userName': user['name'], 'statusesCount': user['statuses_count'], 'friendsCount': user['friends_count'], 'followersCount': user['followers_count'], 'description': user['description']})
                    return json.dumps(users)
                elif model == 'person':
                    nickname = urllib2.unquote(request.form['nickname'])
                    users = []
                    return json.dumps(users)
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
                    page = int(request.form['page'])
                    if page == 1:
                        startoffset = 0
                    else:
                        startoffset = (page - 1) * COUNT_PER_PAGE
                    endoffset = startoffset + COUNT_PER_PAGE - 1
                    province = request.form['province']
                    count, get_results = xapian_search_user.search(query={'province': province}, sort_by=['_id', 'followers_count'], max_offset=10000, fields=['name', 'statuses_count', 'friends_count', 'followers_count', 'profile_image_url', 'description'])
                    users = []
                    offset = 0
                    for r in get_results():
                        if offset >= startoffset and offset <= endoffset:
                            users.append({'id': r['_id'], 'profileImageUrl': r['profile_image_url'], 'userName': r['name'], 'statusesCount': r['statuses_count'], 'friendsCount': r['friends_count'], 'followersCount': r['followers_count'], 'description': r['description']})
                        if offset >= endoffset:
                            break
                        offset += 1
                    return json.dumps(users)
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
                            elif model in fields_value:
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

@mod.route('/group/<fieldEnName>', methods=['GET', 'POST'])
def profile_group(fieldEnName):
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            field = fields_value
            return render_template('profile/profile_group.html', field=field, model=fieldEnName, atfield=unicode(fieldsEn2Zh(fieldEnName), 'utf-8'))
        else:
            pas = db.session.query(UserList).filter(UserList.id==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.profile
                    if identy == 1:
                        field = fields_value
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
                user = xapian_search_domain.search_by_id(uid, fields=['domain'])
                if user:
                    domain = user['domain']
                    d_ = domain.split(',')
                    if len(d_) == 2:
                        field1, field2 = d_
                        field1 = unicode(fieldsEn2Zh(fields_value[int(field1)-1]), 'utf-8')
                        field2 = unicode(ieldsEn2Zh(fields_value[int(field2)-1]), 'utf-8')
                    else:
                        field1 = d_[0]
                        field1 = unicode(fieldsEn2Zh(fields_value[int(field1)-1]), 'utf-8')
                        field2 = unicode('未知', 'utf-8')
                else:
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
                            user = xapian_search_domain.search_by_id(uid, fields=['domain'])
                            if user:
                                domain = user['domain']
                                d_ = domain.split(',')
                                if len(d_) == 2:
                                    field1, field2 = d_
                                    field1 = unicode(fieldsEn2Zh(fields_value[int(field1)-1]), 'utf-8')
                                    field2 = unicode(ieldsEn2Zh(fields_value[int(field2)-1]), 'utf-8')
                                else:
                                    field1 = d_[0]
                                    field1 = unicode(fieldsEn2Zh(fields_value[int(field1)-1]), 'utf-8')
                                    field2 = unicode('未知', 'utf-8')
                            else:
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
        result_arr = []
        interval = None
        topic_limit = None
        keyword_limit = 100
        action = 'nonupdate'
        window_size = 24*60*60
        current_date = date.today().isoformat()
        start_timestamp = 1356969600#2013-01-01
        if request.args.get('interval') and request.args.get('topic_limit') and request.args.get('keyword_limit') and request.args.get('action'):
            interval =  int(request.args.get('interval'))
            topic_limit =  int(request.args.get('topic_limit'))
            keyword_limit = int(request.args.get('keyword_limit'))
            action = request.args.get('action')
        result = db.session.query(PersonalLdaWords).filter((PersonalLdaWords.windowTimestamp==interval*window_size) & (PersonalLdaWords.startTimestamp==start_timestamp)).first()
        if result:
            lda_results = result.word
            return lda_results
        startstr = date.fromtimestamp(start_timestamp).isoformat()
        endstr = date.fromtimestamp(start_timestamp + interval*window_size).isoformat()
        lda_results = lda_topic(uid, startstr, endstr)
        lda_word = PersonalLdaWords(uid=uid, windowTimestamp=interval*window_size, startTimestamp=start_timestamp, word=json.dumps(lda_results))
        db.session.add(lda_word)
        db.session.commit()
        return json.dumps(lda_results)
    else:
        return json.dumps([])

@mod.route('/person_network/<friendship>/<uid>', methods=['GET', 'POST'])
def profile_network(friendship, uid):
    if request.method == 'GET':
        total_days = 89
        today = datetime.datetime.today()
        now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
        now_ts = int(now_ts)
        during = 24 * 3600

        if request.args.get('interval'):
            total_days = int(request.args.get('interval')) - 1
        fri_fol = []
        if friendship == 'friends':
            user = xapian_search_user.search_by_id(int(uid), fields=['friends'])
            friends = user['friends']
            fri_fol = friends
        if friendship == 'followers':
            user = xapian_search_user.search_by_id(int(uid), fields=['followers'])
            followers = user['followers']
            fri_fol = followers
        uid_interact_count = {}
        for i in xrange(-total_days + 1, 1):
            lt = now_ts + during * i
            query_dict = {'user':int(uid),'timestamp': {'$gt': now_ts, '$lt': lt}}
            count,mid_result = xapian_search_weibo.search(query=query_dict,fields=['retweeted_status'])
            mid_list = []
            for i in mid_result():
                if i['retweeted_status'] == None:
                    pass
                else:
                    mid_list.append(i['retweeted_status'])
            for mid in mid_list:
                count,get_results = xapian_search_weibo.search(query={'mid':mid,'retweeted_status':None},fields=['user'])
                for i in get_results():
                    f_uid = i['user']
                if f_uid in fri_fol  and count > 0:
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
        emoticons = emoticon_find(r['text'])
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
        return json.dumps(result_arr)
    else:
        return json.dumps([])

    return json.dumps(result_arr)

@mod.route('/group_count/<fieldEnName>', methods=['GET', 'POST'])
def group_status_count(fieldEnName):
    total_days = 89
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(2013, 4, 10, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600

    time_arr = []
    total_arr = []
    repost_arr = []
    fipost_arr = []

    interval = None
    if request.args.get('interval'):
        interval =  int(request.args.get('interval'))
        total_days = interval - 180

    startoffset = 0
    endoffset = 10000
    fields_set = getFieldUsersByScores(fieldEnName, startoffset, endoffset)
    for i in xrange(-total_days + 1, 1, 7):
        lt = now_ts + during * i
        post_count = {}
        fipost_count = 0
        repost_count = 0
        for uid in fields_set:
            query_dict = {
                'timestamp': {'$gt': lt, '$lt': lt + during},
                'user': uid
            }
            count, retweeted_status = xapian_search_weibo.search(query=query_dict, fields=['retweeted_status'])
            for i in retweeted_status():
                if i['retweeted_status'] == None:
                    fipost_count += 1
                else:
                    repost_count += 1
        sumcount = fipost_count + repost_count
        if sumcount > 0:
            time_arr.append(ts2date(lt).isoformat())
            total_arr.append(sumcount)
            repost_arr.append(repost_count)
            fipost_arr.append(fipost_count)
    return json.dumps({'time': time_arr, 'count': total_arr, 'repost': repost_arr, 'fipost': fipost_arr})

@mod.route('/group_active/<fieldEnName>', methods=['GET', 'POST'])
def group_active_count(fieldEnName):
    total_days = 89
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600

    interval = 6
    if request.args.get('interval'):
        interval =  int(request.args.get('interval'))
    total_days = interval
    startoffset = 0
    endoffset = 10000
    fields_set = getFieldUsersByScores(fieldEnName, startoffset, endoffset)
    user_count = {}
    for uid in fields_set:
        query_dict = {
            'timestamp': {'$gt': now_ts - during * total_days, '$lt': now_ts},
            'user': uid
        }
        count = xapian_search_weibo.search(query=query_dict, count_only=True)
        user_count[uid] = count
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
    total_days = 270
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(2013, 4, 10, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600

    time_arr = []
    happy_arr = []
    angry_arr = []
    sad_arr = []

    interval = None
    if request.args.get('interval'):
        interval =  int(request.args.get('interval'))
        total_days = interval - 180

    startoffset = 0
    endoffset = 10000
    fields_set = getFieldUsersByScores(fieldEnName, startoffset, endoffset)
    for i in xrange(-total_days + 1, 1, 7):
        lt = now_ts + during * i
        emotion_count = {}
        for emotion in emotions_kv.values():
            e_count = 0
            for uid in fields_set:
                query_dict = {
                    'timestamp': {'$gt': lt, '$lt': lt + during},
                    'sentiment': emotion,
                    'user': uid
                }
                e_count += xapian_search_sentiment.search(query=query_dict, count_only=True)
            emotion_count[emotion] = e_count
        if sum(emotion_count.values()) > 0:
            sumcount = sum(emotion_count.values())
            time_arr.append(ts2date(lt).isoformat())
            happy_arr.append(int(emotion_count[1] * 100 / sumcount) / 100.0)
            angry_arr.append(int(emotion_count[2] * 100 / sumcount) / 100.0)
            sad_arr.append(int(emotion_count[3] * 100 / sumcount) / 100.0)
    return json.dumps({'time': time_arr, 'happy': happy_arr, 'angry': angry_arr, 'sad': sad_arr})

@mod.route('/group_verify/<fieldEnName>')
def profile_group_verify(fieldEnName):
    startoffset = 0
    endoffset = 10000
    fields_set = getFieldUsersByScores(fieldEnName, startoffset, endoffset)
    verified_count = 0
    uverified_count = 0
    for uid in fields_set:
        user = xapian_search_user.search_by_id(uid, fields=['verified'])
        if user['verified']:
            verified_count += 1
        else:
            uverified_count += 1
    result_list = ''
    if sum([verified_count, uverified_count]) > 0:
        sumcount = sum([verified_count, uverified_count])
        result_list = str(verified_count) + ',' + str(uverified_count) + ',' + str(int(verified_count * 100 / sumcount) / 100.00) + ',' +  str(1 - int(verified_count * 100 / sumcount) / 100.00)

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
    city_count = {}
    startoffset = 0
    endoffset = 10000
    fields_set = getFieldUsersByScores(fieldEnName, startoffset, endoffset)
    for uid in fields_set:
        user = xapian_search_user.search_by_id(uid, fields=['location'])
        province = user['location'].split(' ')[0]
        try:
            city_count[province] += 1
        except:
            city_count[province] = 1
    results = province_color_map(city_count)
    return json.dumps(results)