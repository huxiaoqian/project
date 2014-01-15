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
import leveldb
import urllib2
import operator
from utils import last_day, merge, getUsersInfoByUidInteract
from weibo.model import *
from weibo.extensions import db
from datetime import date, datetime
from city_color import province_color_map
from ldamodel import lda_topic
from operator import itemgetter
from flask.ext import admin
from person import _search_person_basic, _search_person_important_active
from flask.ext.sqlalchemy import Pagination
from time_utils import datetimestr2ts
from flask import Flask, url_for, render_template, request, make_response, flash, abort, Blueprint, session, redirect
from utils import acquire_topic_id, read_rank_results, pagerank_rank, degree_rank, make_network_graph, get_above100_weibos, weiboinfo2url, emoticon_find, ts2hour
from utils import ts2date, getFieldUsersByScores, datetime2ts, last_day, ts2datetime, ts2HMS, last_week_to_date, getUserNameById, getUserIdByName
from weibo.global_config import xapian_search_user, xapian_search_weibo, xapian_search_domain, LEVELDBPATH, \
                                fields_value, fields_id, emotions_zh_kv, emotions_kv
from _leveldb import getPersonData, getDomainKeywordsData, getDomainBasic, getDomainCountData

buckets = {}
mod = Blueprint('profile', __name__, url_prefix='/profile')
COUNT_PER_PAGE = 20
month_value = {'January':1, 'February':2, 'March':3, 'April':4, 'May':5, 'June':6, 'July':7, 'August':8, 'September':9, 'October':10, 'November':11, 'December':12}
fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology', 'oversea']
labels = ['university', 'homeadmin', 'abroadadmin', 'homemedia', 'abroadmedia', 'folkorg', \
          'lawyer', 'politician', 'mediaworker', 'activer', 'grassroot', 'other']
DOMAIN_LIST = fields_value + labels


def _utf_encode(s):
    if isinstance(s, str):
        return s
    else:
        return s.encode('utf-8')

def _utf_decode(s):
    if isinstance(s, str):
        return s.decode('utf-8')
    else:
        return s

def _time_zone(stri):
    '''时间段参数从前台时间控件传来
    '''
    dates = stri.split(' - ')
    tslist = []

    for date in dates:
        month_day, year = date.split(',')
        month, day = month_day.split('月 ')
        year = int(year)
        month = int(month)
        day = filter(str.isdigit, day)#只保留数字，去掉“日”
        day = int(day)
        ts = datetime(year, month, day, 0, 0, 0)
        ts = time.mktime(ts.timetuple())
        tslist.append(ts)

    start_ts = tslist[0]
    end_ts = tslist[1]

    return int(start_ts), int(end_ts)

def get_bucket(bucket):
    if bucket in buckets:
        return buckets[bucket]
    buckets[bucket] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_' + bucket), block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return buckets[bucket]


def fieldsEn2Zh(name):
    if name == 'finance':
        return u'财经'
    if name == 'media':
        return u'媒体'
    if name == 'culture':
        return u'文化'
    if name == 'technology':
        return u'科技'
    if name == 'entertainment':
        return u'娱乐'
    if name == 'education':
        return u'教育'
    if name == 'fashion':
        return u'时尚'
    if name == 'sports':
        return u'体育'
    if name == 'oversea':
        return u'境外'
    if name == 'university':
        return u'高校微博'
    if name == 'homeadmin':
        return u'境内机构'
    if name == 'abroadadmin':
        return u'境外机构'
    if name == 'homemedia':
        return u'境内媒体'
    if name == 'abroadmedia':
        return u'境外媒体'
    if name == 'folkorg':
        return u'民间组织'
    if name == 'lawyer':
        return u'律师'
    if name == 'politician':
        return u'政府官员'
    if name == 'mediaworker':
        return u'媒体人士'
    if name == 'activer':
        return u'活跃人士'
    if name == 'grassroot':
        return u'草根'
    if name == 'other':
        return u'其它'

def getStaticInfo():
    statuscount = [0, 2000000, 4000000, 6000000, 8000000, 10000000, 12000000, 14000000, 16000000, 18000000, 20000000]
    friendscount = [0, 400, 800, 1200, 1600, 2000, 2400, 2800, 3200, 3600, 4000]
    followerscount = [0, 6000000, 12000000, 18000000, 24000000, 30000000, 36000000, 42000000, 48000000, 54000000, 60000000]
    province = ['北京',  '上海', '香港', '台湾', '重庆', '澳门', '天津', '江苏', '浙江', '四川', '江西', '福建', '青海', '吉林', '贵州', '陕西', '山西', '河北', '湖北', '辽宁', '湖南', '山东', '云南', '河南', '广东', '安徽', '甘肃', '海南', '黑龙江', '内蒙古', '新疆', '广西', '宁夏', '西藏', '海外']
    statusRange = [{'lowBound': statuscount[i], 'upBound': statuscount[i+1]} for i in range(len(statuscount)-1)]
    friendsRange = [{'lowBound': friendscount[i], 'upBound': friendscount[i+1]} for i in range(len(friendscount)-1)]
    followersRange = [{'lowBound': followerscount[i], 'upBound': followerscount[i+1]} for i in range(len(followerscount)-1)]
    province = [{'province': unicode(i, 'utf-8')} for i in province]
    fields = [{'fieldEnName': f, 'fieldZhName': fieldsEn2Zh(f)} for f in fields_value]
    return statusRange, friendsRange, followersRange, province, fields

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

#添加画像主页
@mod.route('/', methods=['GET','POST'])
def test_index():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            return render_template('profile/index.html')
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            #pas = db.session.query(UserList).filter(UserList.id==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.identify
                    if identy == 1:
                        return render_template('profile/index.html')
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')
#添加画像主页

@mod.route('/search/', methods=['GET', 'POST'])
@mod.route('/search/<model>', methods=['GET', 'POST'])
def profile_search(model='hotest'):
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == 'GET':
                statuscount, friendscount, followerscount, province, field = getStaticInfo()
                province_str = ''
                province = ['北京',  '上海', '香港', '台湾', '重庆', '澳门', '天津', '江苏', '浙江', '四川', '江西', '福建', '青海', '吉林', '贵州', '陕西', '山西', '河北', '湖北', '辽宁', '湖南', '山东', '云南', '河南', '广东', '安徽', '甘肃', '海南', '黑龙江', '内蒙古', '新疆', '广西', '宁夏', '西藏', '海外'] 
                for pro in province:
                    province_str += unicode(pro, 'utf-8') + ','
                if model == 'person':
                    nickname = urllib2.unquote(request.args.get('nickname'))
                    print nickname.encode('utf-8')
                    return render_template('profile/profile_search.html',statuscount=statuscount,
                                           friendscount=friendscount, followerscount=followerscount,
                                           location=province_str, field=field, model=model, result=None, nickname=nickname)
                elif model == 'find':
                    statuses_count_upBound = request.args.get('statuses_count_upBound',None)
                    friends_count_upBound = request.args.get('friends_count_upBound',None)
                    followers_count_upBound = request.args.get('followers_count_upBound',None)
                    top_n = request.args.get('search_top_n',None)
                    province_str = request.args.get('province_str',None)
                    rank_str = request.args.get('rankcount',None)
                    if(top_n):
                        result_count = int(top_n)
                    else:
                        result_count = 100
                    if(statuses_count_upBound):
                        statusescount_up = (int(statuses_count_upBound))*10000
                    else:
                        statusescount_up = 2000000

                    if(friends_count_upBound):
                        friendscount_up = int(friends_count_upBound)
                    else:
                        friendscount_up = 400

                    if(followers_count_upBound):
                        followerscount_up = (int(followers_count_upBound))*10000
                    else:
                        followerscount_up = 6000000

                    return render_template('profile/profile_search.html',result_count = result_count, statusescount_up = statusescount_up,
                                           friendscount_up = friendscount_up,followerscount_up = followerscount_up,
                                           location = province_str, field=field, model = model,rankcount=rank_str)  
                else:
                    return render_template('profile/profile_search.html',statuscount=statuscount,
                                           friendscount=friendscount, followerscount=followerscount,
                                           location=province_str, field=field, model=model, result=None)
            if request.method == 'POST' and request.form['page']:
                if model == 'newest':
                    page = int(request.form['page'])
                    if page == 1:
                        startoffset = 0
                    else:
                        startoffset = (page - 1) * COUNT_PER_PAGE

                    total_days = 90
                    begin_ts = int(time.mktime(datetime(2013,1, 1, 2, 0).timetuple()))
                    now_ts = int(time.mktime(datetime(2014, 1, 1, 2, 0).timetuple()))
                    during = 24 * 3600
                    total_days = (now_ts - begin_ts) / during
                    query_dict = {
                        'created_at': {
                            '$gt': begin_ts,
                            '$lt': now_ts,
                        }
                    }
                    count, get_results = xapian_search_user.search(query=query_dict, start_offset=startoffset, max_offset=COUNT_PER_PAGE,
                                                                   fields=['created_at', '_id', 'name', 'statuses_count', 'followers_count', 'friends_count', 'description', 'profile_image_url'], 
                                                                   sort_by=['created_at'])
                    users = []
                    print count
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
                elif model == 'oversea':
                    page = int(request.form['page'])
                    if page == 1:
                        startoffset = 0
                    else:
                        startoffset = (page - 1) * COUNT_PER_PAGE
                    endoffset = startoffset + COUNT_PER_PAGE - 1
                    province = '海外'
                    count, get_results = xapian_search_user.search(query={'location': province}, sort_by=['followers_count'], max_offset=10000, fields=['_id', 'name', 'statuses_count', 'friends_count', 'followers_count', 'profile_image_url', 'description'])
                    users = []
                    offset = 0
                    for r in get_results():
                        if offset >= startoffset and offset <= endoffset:
                            users.append({'id': r['_id'], 'profileImageUrl': r['profile_image_url'], 'userName': r['name'], 'statusesCount': r['statuses_count'], 'friendsCount': r['friends_count'], 'followersCount': r['followers_count'], 'description': r['description']})
                        if offset >= endoffset:
                            break
                        offset += 1
                    return json.dumps(users)
                elif model in fields_value:
                    page = int(request.form['page'])
                    if page == 1:
                        startoffset = 0
                    else:
                        startoffset = (page - 1) * COUNT_PER_PAGE
                    endoffset = startoffset + COUNT_PER_PAGE - 1
                    fieldEnName = model
                    count, field_users = xapian_search_domain.search(query={'domain':str(fields_id[str(fieldEnName)])}, sort_by=['followers_count'], fields=['_id', 'name', 'statuses_count', 'friends_count', 'followers_count', 'profile_image_url', 'description'], max_offset=10000)
                    users = []
                    count = 0
                    for field_user in field_users():#[startoffset: endoffset]:
                        if count < startoffset:
                            count += 1
                            continue
                        if count > endoffset:
                            break
                        field_user['id'] = field_user['_id']
                        field_user['profileImageUrl'] = field_user['profile_image_url']
                        field_user['userName'] = field_user['name']
                        field_user['statusesCount'] = field_user['statuses_count']
                        field_user['friendsCount'] = field_user['friends_count']
                        field_user['followersCount'] = field_user['followers_count']
                        field_user['description'] = field_user['description']
                        users.append(field_user)
                        count += 1
                    return json.dumps(users)
                elif model == 'person':
                    nickname = urllib2.unquote(request.form['nickname'])
                    uid = getUidByName(nickname)
                    users = []
                    if uid:
                        user = getUserInfoById(uid)
                        if user:
                            users.append(user)                    
                    return json.dumps(users)
                elif model == 'find':
                    page = int(request.form['page'])
                    if page == 1:
                        startoffset = 0
                    else:
                        startoffset = (page - 1) * COUNT_PER_PAGE
                    endoffset = startoffset + COUNT_PER_PAGE - 1
                    result_count = int(request.form['result_count'])
                    statusescount_up = int(request.form['statusescount_up'])
                    friendscount_up = int(request.form['friendscount_up'])
                    followerscount_up = int(request.form['followerscount_up'])
                    province_str = request.form['province']
                    rankcount = request.form['rankcount']
                    province = province_str.split(',')
                    query_dict = {}
                    query_dict['$and'] = []
                    query_dict['$and'].append({'statuses_count':{'$gt': statusescount_up - 2000000,'$lt': statusescount_up }})
                    query_dict['$and'].append({'followers_count':{'$gt': followerscount_up - 6000000 ,'$lt': followerscount_up}})
                    query_dict['$and'].append({'friends_count':{'$gt': friendscount_up - 400 ,'$lt': friendscount_up}})
                    or_dict = {}
                    or_dict['$or'] = []
                    for pro in province:
                        or_dict['$or'].append({'location': pro})
                    query_dict['$and'].append(or_dict)
                    count, get_results = xapian_search_user.search(query=query_dict, max_offset=result_count,
                                                                   fields=[ '_id', 'name', 'statuses_count', 'followers_count', 'friends_count', 'description', 'profile_image_url'], 
                                                                   sort_by=[rankcount])
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
                    return json.dumps(users[startoffset:endoffset])
        else:
            #pas = db.session.query(UserList).filter(UserList.id==session['user']).all()
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.profile
                    if identy == 1:
                        if request.method == 'GET':
                            statuscount, friendscount, followerscount, province, field = getStaticInfo()
                            province_str = ''
                            province = ['北京',  '上海', '香港', '台湾', '重庆', '澳门', '天津', '江苏', '浙江', '四川', '江西', '福建', '青海', '吉林', '贵州', '陕西', '山西', '河北', '湖北', '辽宁', '湖南', '山东', '云南', '河南', '广东', '安徽', '甘肃', '海南', '黑龙江', '内蒙古', '新疆', '广西', '宁夏', '西藏', '海外'] 
                            for pro in province:
                                province_str += unicode(pro, 'utf-8') + ','
                            if model == 'person':
                                nickname = urllib2.unquote(request.args.get('nickname'))
                                return render_template('profile/profile_search.html',statuscount=statuscount,
                                                       friendscount=friendscount, followerscount=followerscount,
                                                       location=province_str, field=field, model=model, result=None, nickname=nickname)
                            elif model == 'find':
                                statuses_count_upBound = request.args.get('statuses_count_upBound',None)
                                friends_count_upBound = request.args.get('friends_count_upBound',None)
                                followers_count_upBound = request.args.get('followers_count_upBound',None)
                                top_n = request.args.get('search_top_n',None)
                                province_str = request.args.get('province_str',None)
                                rank_str = request.args.get('rankcount',None)
                                if(top_n):
                                    result_count = int(top_n)
                                else:
                                    result_count = 100
                                if(statuses_count_upBound):
                                    statusescount_up = (int(statuses_count_upBound))*10000
                                else:
                                    statusescount_up = 2000000

                                if(friends_count_upBound):
                                    friendscount_up = int(friends_count_upBound)
                                else:
                                    friendscount_up = 400

                                if(followers_count_upBound):
                                    followerscount_up = (int(followers_count_upBound))*10000
                                else:
                                    followerscount_up = 6000000

                                return render_template('profile/profile_search.html',result_count = result_count, statusescount_up = statusescount_up,
                                                       friendscount_up = friendscount_up,followerscount_up = followerscount_up,
                                                       location = province_str, field=field, model = model,rankcount=rank_str)  
                            else:
                                return render_template('profile/profile_search.html',statuscount=statuscount,
                                                       friendscount=friendscount, followerscount=followerscount,
                                                       location=province_str, field=field, model=model, result=None)
                        if request.method == 'POST' and request.form['page']:
                            if model == 'newest':
                                page = int(request.form['page'])
                                if page == 1:
                                    startoffset = 0
                                else:
                                    startoffset = (page - 1) * COUNT_PER_PAGE

                                total_days = 90
                                today = datetime.today()
                                now_ts = time.mktime(datetime(today.year, today.month, today.day, 2, 0).timetuple())
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
                            elif model == 'find':
                                page = int(request.form['page'])
                                if page == 1:
                                    startoffset = 0
                                else:
                                    startoffset = (page - 1) * COUNT_PER_PAGE
                                endoffset = startoffset + COUNT_PER_PAGE - 1
                                result_count = int(request.form['result_count'])
                                statusescount_up = int(request.form['statusescount_up'])
                                friendscount_up = int(request.form['friendscount_up'])
                                followerscount_up = int(request.form['followerscount_up'])
                                province_str = request.form['province']
                                rankcount = request.form['rankcount']
                                province = province_str.split(',')
                                query_dict = {}
                                query_dict['$and'] = []
                                query_dict['$and'].append({'statuses_count':{'$gt': statusescount_up - 2000000,'$lt': statusescount_up }})
                                query_dict['$and'].append({'followers_count':{'$gt': followerscount_up - 6000000 ,'$lt': followerscount_up}})
                                query_dict['$and'].append({'friends_count':{'$gt': friendscount_up - 400 ,'$lt': friendscount_up}})
                                or_dict = {}
                                or_dict['$or'] = []
                                for pro in province:
                                    or_dict['$or'].append({'location': pro})
                                query_dict['$and'].append(or_dict)
                                count, get_results = xapian_search_user.search(query=query_dict, max_offset=result_count,
                                                                               fields=[ '_id', 'name', 'statuses_count', 'followers_count', 'friends_count', 'description', 'profile_image_url'], 
                                                                               sort_by=[rankcount])
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
                                return json.dumps(users[startoffset:endoffset])
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route('/group/<fieldEnName>', methods=['GET', 'POST'])
def profile_group(fieldEnName):
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            field = [{'fieldEnName': f, 'fieldZhName': fieldsEn2Zh(f)} for f in fields_value]
            return render_template('profile/profile_group.html', field=field, model=fieldEnName, atfield=fieldsEn2Zh(fieldEnName))
        else:
            pas = db.session.query(UserList).filter(UserList.id==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.profile
                    if identy == 1:
                        field = [{'fieldEnName': f, 'fieldZhName': fieldsEn2Zh(f)} for f in fields_value]
                        return render_template('profile/profile_group.html', field=field, model=fieldEnName, atfield=fieldsEn2Zh(fieldEnName))
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route('/group/',methods=['GET','POST'])
def test_profile_group():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == 'GET':
                fieldEnName = request.args.get('fieldEnName',None)
                during_time = request.args.get('during_time',None)
                during_date = _utf_encode(during_time)
                start_ts,end_ts = _time_zone(during_date)
                window_size = (end_ts - start_ts)/(24*3600)
            field = [{'fieldEnName': f, 'fieldZhName': fieldsEn2Zh(f)} for f in fields_value]
            return render_template('profile/profile_group.html', field=field, model=fieldEnName, atfield=fieldsEn2Zh(fieldEnName))
        else:
            pas = db.session.query(UserList).filter(UserList.id==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.profile
                    if identy == 1:
                        field = [{'fieldEnName': f, 'fieldZhName': fieldsEn2Zh(f)} for f in fields_value]
                        return render_template('profile/profile_group.html', field=field, model=fieldEnName, atfield=fieldsEn2Zh(fieldEnName))
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
                    current_time = '20130904'
                    active, important, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict = getPersonData(uid, current_time)
                    for r in get_results():
                        user = {'id': uid, 'profile_image_url': r['profile_image_url'], 'userName':  _utf_8_decode(r['name']), 'friends_count': r['friends_count'], \
                                'statuses_count': r['statuses_count'], 'followers_count': r['followers_count'], 'gender': r['gender'], \
                                'verified': r['verified'], 'created_at': r['created_at'], 'location': _utf_8_decode(r['location'])}
                        user['created_at'] = ts2HMS(user['created_at']);
                        user['active_rank'] = active
                        user['important_rank'] = important
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
# def profile_person(uid):
#     if 'logged_in' in session and session['logged_in']:
#         if session['user'] == 'admin':
#             if uid != None:
#                 status1, personbasic = _search_person_basic(uid)
#                 # status2, person_important_active = _search_person_important_active(uid)
#                 user = {}
#                 if status1 == 'success':
#                     verifiedTypenum = personbasic.verifiedType
#                     friendsCount = personbasic.friendsCount
#                     followersCount = personbasic.followersCount
#                     statuseCount = personbasic.statusesCount
#                     created_at = time.strftime("%m月 %d日, %Y", time.localtime(personbasic.created_at))
#                     user = {'id': personbasic.userId, 'profile_image_url': personbasic.profileImageUrl, 'userName':  _utf_8_decode(personbasic.name), \
#                     'friends_count': friendsCount, 'statuses_count': statuseCount, 'followers_count': followersCount, \
#                     'gender': personbasic.gender, 'verified': personbasic.verified, 'created_at': _utf_8_decode(created_at), \
#                     'location': _utf_8_decode(personbasic.location), 'date': personbasic.date, \
#                     'verifiedTypenum': verifiedTypenum, 'description': _utf_8_decode(personbasic.description)}
#                     user['active_rank'] = 0
#                     user['important_rank'] = 0
#                 else:
#                     return 'no such user'
#                 # if status2 == 'success':
#                 #     active = person_important_active.activeSeries.split('_')[-1]
#                 #     important = person_important_active.importantSeries.split('_')[-1]
#                 #     user['active_rank'] = active
#                 #     user['important_rank'] = important
#             return render_template('profile/profile_person.html', user=user)
#         else:
#             pas = db.session.query(UserList).filter(UserList.id==session['user']).all()
#             if pas != []:
#                 for pa in pas:
#                     identy = pa.profile
#                     if identy == 1:
#                         if userId != None:
#                             status1, personbasic = _search_person_basic(userId)
#                             # status2, person_important_active = _search_person_important_active(userId)
#                             user = {}
#                             if status1 == 'success':
#                                 provincenum = personbasic.province
#                                 citynum = personbasic.city
#                                 verifiedTypenum = personbasic.verifiedType
#                                 friendsCount = personbasic.friendsCount
#                                 followersCount = personbasic.followersCount
#                                 statuseCount = personbasic.statuseCount
#                                 created_at = time.strftime("%m月 %d日, %Y", time.localtime(personbasic.created_at))
#                                 user = {'id': personbasic.userId, 'profile_image_url': personbasic.profileImageUrl, 'userName':  _utf_8_decode(personbasic.name), \
#                                 'friends_count': friendsCount, 'statuses_count': statuseCount, 'followers_count': followersCount, \
#                                 'gender': personbasic.gender, 'verified': personbasic.verified, 'created_at': created_at, \
#                                 'location': _utf_8_decode(personbasic.location), 'provincenum': provincenum, 'citynum': citynum, \
#                                 'verifiedTypenum': verifiedTypenum, 'description': _utf_8_decode(personbasic.description),\
#                                 'date': personbasic.date}
#                             else:
#                                 return 'no such user'
#                             # if status2 == 'success':
#                             #     active = person_important_active.activeSeries.split('_')[-1]
#                             #     important = person_important_active.importantSeries.split('_')[-1]
#                             #     user['active_rank'] = active
#                             #     user['important_rank'] = important
#                         return render_template('profile/profile_person.html', user=user)
#                     else:
#                         return redirect('/')
#             return redirect('/')
#     else:
#         return redirect('/')


def getFriendship(uid, schema='friends'):
    if uid:
        user = xapian_search_user.search_by_id(int(uid), fields=[schema])
        if user:
            return user[schema]
        else:
            return []
    else:
        return []


def getUidByMid(mid):
    weibo = xapian_search_weibo.search_by_id(int(mid), fields=['user'])
    if weibo:
        return weibo['user']
    else:
        return None

def getUidByName(name):
    count, users = xapian_search_user.search(query={'name': name}, fields=['_id'])
    if count:
        for user in users():
            return user['_id']
    else:
        return None


def getInteractCount(uid, start_ts, end_ts, schema='repost', amongfriends=True, amongfollowers=True):
    fri_fol = []
    if amongfriends:
        friends = getFriendship(uid, 'friends')
        fri_fol.extend(friends)
    if amongfollowers:
        followers = getFriendship(uid, 'followers')
        fri_fol.extend(followers)

    interact_dict = {}
    if uid:
        count, results = xapian_search_weibo.search(query={'user': int(uid), 'timestamp': {'$gt': start_ts, '$lt': end_ts}}, fields=['text', 'retweeted_mid'])
        
        if schema == 'repost':
            for r in results():
                text = r['text']
                repost_users = re.findall(u'//@([a-zA-Z-_\u0391-\uFFE5]+)', text)
                for name in repost_users:
                    repost_uid = getUidByName(name)
                    if repost_uid:
                        try:
                            interact_dict[repost_uid] += 1
                        except KeyError:
                            interact_dict[repost_uid] = 1
                retweeted_mid = r['retweeted_mid']
                retweeted_uid = getUidByMid(retweeted_mid)
                if retweeted_uid:
                    try:
                        interact_dict[retweeted_uid] += 1
                    except KeyError:
                        interact_dict[retweeted_uid] = 1

        elif schema == 'at':
            for r in results():
                text = r['text']
                at_users = re.findall(u'@([a-zA-Z-_\u0391-\uFFE5]+)', text)
                for name in at_users:
                    at_uid = getUidByName(name)
                    if at_uid:
                        try:
                            interact_dict[at_uid] += 1
                        except KeyError:
                            interact_dict[at_uid] = 1
                retweeted_mid = r['retweeted_mid']
                retweeted_uid = getUidByMid(retweeted_mid)
                if retweeted_uid:
                    try:
                        interact_dict[retweeted_uid] += 1
                    except KeyError:
                        interact_dict[retweeted_uid] = 1
    
    results = {}

    if fri_fol != []:
        for k, v in interact_dict.iteritems():
            if k in fri_fol:
                results[k] = v
    else:
        results = interact_dict
    
    return results


@mod.route('/person_interact_network/<uid>', methods=['GET', 'POST'])
def profile_interact_network(uid):
    if request.method == 'GET':
        total_days = 270
        today = datetime.today()
        now_ts = time.mktime(datetime(today.year, today.month, today.day, 2, 0).timetuple())
        now_ts = int(now_ts)
        during = 24 * 3600

        if request.args.get('interval'):
            total_days =  int(request.args.get('interval')) - 1   
        
        center_uid = uid
        fri_fol = []
        friends = getFriendship('friends')
        followers = getFriendship('followers')
        fri_fol.extend(friends)
        fri_fol.extend(followers)

    
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
                        field1 = fieldsEn2Zh(fields_value[int(field1)-1])
                        field2 = fieldsEn2Zh(fields_value[int(field2)-1])
                    else:
                        field1 = d_[0]
                        field1 = fieldsEn2Zh(fields_value[int(field1)-1])
                        field2 = u'未知'
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
            elif model == 'grouplocation':
                return render_template('profile/ajax/group_location.html', field=uid)
            elif model == 'groupimportant':
                return render_template('profile/ajax/group_important.html', field=uid)
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
                                    field1 = fieldsEn2Zh(fields_value[int(field1)-1])
                                    field2 = fieldsEn2Zh(fields_value[int(field2)-1])
                                else:
                                    field1 = d_[0]
                                    field1 = fieldsEn2Zh(fields_value[int(field1)-1])
                                    field2 = u'未知'
                            else:
                                field1 = u'未知'
                                field2 = u'未知'
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
        limit = 50

        # 暂定取20130901数据
        time_str = '20130901'

        active, important, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict = getPersonData(uid, time_str)
        if keywords_dict:
            sortedkeywords = sorted(keywords_dict.iteritems(), key=operator.itemgetter(1), reverse=True)
            for k, v in sortedkeywords[:limit]:
                result_arr.append({'text': k, 'size': float(v)})
        
        return json.dumps({'status': 'current finished', 'data': result_arr})

    else:
        return json.dumps([])
    return json.dumps(result_arr)

@mod.route('/person_network/<friendship>/<uid>', methods=['GET', 'POST'])
def profile_network(friendship, uid):
    if request.method == 'GET':
        limit = 10
        interval = 7

        fri_fol = []
        user = xapian_search_user.search_by_id(int(uid), fields=[friendship])
        if user:
            fri_fol = user[friendship]

        direct_uid_interact_count = {}
        retweeted_uid_interact_count = {}
        retweeted_friends_interact_count = {}
        uid_interact_count = {}

        datestr = '20130907'
        date_list = last_week_to_date(datestr, interval)
    
        for datestr in date_list:
            active, important, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict = getPersonData(uid, datestr)
            for k, v in retweeted_interact.iteritems():
                k = int(k)
                v = int(v)
                if k in set(fri_fol):
                    try:
                        retweeted_friends_interact_count[k] += v
                    except KeyError:
                        retweeted_friends_interact_count[k] = v

                try:
                    retweeted_uid_interact_count[k] += v
                except KeyError:
                    retweeted_uid_interact_count[k] = v

            for k, v in direct_interact.iteritems():
                v = int(v)
                try:
                    direct_uid_interact_count[k] += v
                except KeyError:
                    direct_uid_interact_count[k] = v

        direct_uid_sorted = sorted(direct_uid_interact_count.iteritems(), key=operator.itemgetter(1), reverse=False)
        retweeted_uid_sorted = sorted(retweeted_uid_interact_count.iteritems(), key=operator.itemgetter(1), reverse=False)
        retweeted_friends_sorted = sorted(retweeted_friends_interact_count.iteritems(), key=operator.itemgetter(1), reverse=False)
        retweeted_uid2name_dict = {}
        for k, v in retweeted_uid_interact_count.iteritems():
            retweeted_uid2name_dict[getUserNameById(k)] = v
        uid_interact_count = merge(direct_uid_interact_count, retweeted_uid2name_dict, lambda x, y: x+y)
        uid_sorted = sorted(uid_interact_count.iteritems(), key=operator.itemgetter(1), reverse=True)

        direct_uid_sorted = direct_uid_sorted[-limit:]
        retweeted_uid_sorted = retweeted_uid_sorted[-limit:]
        retweeted_friends_sorted = retweeted_friends_sorted[-limit:]

        retweeted_name_sorted = []
        for uid, count in retweeted_uid_sorted:
            retweeted_name_sorted.append((getUserNameById(uid), count))

        retweeted_friends_name_sorted = []
        for uid, count in retweeted_friends_sorted:
            retweeted_friends_name_sorted.append((getUserNameById(uid), count))

        users = getUsersInfoByUidInteract(uid_sorted)

        return json.dumps({'status': 'finished', 'data': {'direct_uid': direct_uid_sorted, \
                           'retweeted_friends': retweeted_friends_name_sorted, \
                           'retweeted_uid': retweeted_name_sorted, 'users': users}})

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
                    field = fieldsEn2Zh(field)
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
    if request.args.get('interval'):
        interval =  int(request.args.get('interval'))

    interval = 7
    datestr = '20130907'

    date_list = last_week_to_date(datestr, interval)
    post_status_kv = {'total': 2, 'repost': 1, 'fipost': 0}

    time_arr = []
    post_arr = []
    repost_arr = []
    fipost_arr = []
    im_arr = []
    emot_arr = []

    for datestr in date_list:
        active, important, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict = getPersonData(uid, datestr)
        post_arr.append(active)
        repost_arr.append(reposts)
        fipost_arr.append(original)
        im_arr.append(important)
        emot_arr.append(emoticon)
        time_arr.append(datestr)

    for idx in range(0, len(date_list)):
        if post_arr[idx] != 0:
            total_post_count = post_arr[idx]
            retweets_count = repost_arr[idx]
            emoticons_count = fipost_arr[idx]
            emoticons_ratio = int(emoticons_count * 100 / total_post_count) / 100.0
            retweetes_ratio = int(retweets_count * 100 / total_post_count) / 100.0
        else:
            emoticons_ratio = 0.0
            retweetes_ratio = 0.0

    return json.dumps({'time': time_arr, 'count': post_arr, 'repost': repost_arr, 'fipost': fipost_arr, \
                       'important': im_arr, 'total_tweets': total_post_count, \
                       'retweets_ratio': retweetes_ratio, 'emoticons_ratio': emoticons_ratio})


def _utf_8_decode(stri):
    if isinstance(stri, str):
        return unicode(stri,'utf-8')
    return stri


def getUserInfoById(uid):
    count, get_results = xapian_search_user.search(query={'_id': uid}, fields=['profile_image_url', 'name', 'friends_count', \
                                          'statuses_count', 'followers_count', 'gender', 'verified', 'created_at', 'location', 'description'])
    if count:
        for r in get_results():
            user = {'id': uid, 'profile_image_url': r['profile_image_url'], 'userName':  r['name'], 'friendsCount': r['friends_count'], \
                    'statusesCount': r['statuses_count'], 'followersCount': r['followers_count'], 'gender': r['gender'], \
                    'verified': r['verified'], 'created_at': r['created_at'], 'location': _utf_8_decode(r['location']), 'description': r['description']}
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
        limit = 50
        window_size = 24*60*60
        datestr = '20130901'
        domainid = DOMAIN_LIST.index(fieldEnName)
        keywords_dict = getDomainKeywordsData(domainid, datestr)
        if request.args.get('interval') and request.args.get('sort') and request.args.get('limit') and request.args.get('topic_type'):
            interval =  int(request.args.get('interval'))
            sort =  request.args.get('sort')
            limit = int(request.args.get('limit'))
            topic_type = request.args.get('topic_type')
        if topic_type == 'freq':
            keywords_sorted = sorted(keywords_dict.iteritems(), key=lambda(k, v): v, reverse=False)
            top_keywords = keywords_sorted[len(keywords_sorted)-limit:]
            result_arr = [{'text': k, 'size': float(v)} for k, v in top_keywords]
            return json.dumps({'status': 'current finished', 'data': result_arr})
    else:
        return json.dumps([])

    return json.dumps(result_arr)

@mod.route('/group_count/<fieldEnName>', methods=['GET', 'POST'])
def profile_group_status_count(fieldEnName):
    if request.args.get('interval'):
        interval =  int(request.args.get('interval'))

    interval = 7
    datestr = '20130907'

    date_list = last_week_to_date(datestr, interval)
    domainid = DOMAIN_LIST.index(fieldEnName)

    time_arr = []
    total_arr = []
    repost_arr = []
    fipost_arr = []

    for datestr in date_list:
        active, important, reposts, original = getDomainCountData(domainid, datestr)
        sumcount = reposts + original
        if sumcount > 0:
            time_arr.append(ts2date(datetimestr2ts(datestr)).isoformat())
            total_arr.append(sumcount)
            repost_arr.append(reposts)
            fipost_arr.append(original)

    return json.dumps({'time': time_arr, 'count': total_arr, 'repost': repost_arr, 'fipost': fipost_arr})

@mod.route('/group_important/<fieldEnName>', methods=['GET', 'POST'])
def group_active_count(fieldEnName):
    if request.args.get('interval'):
        interval =  int(request.args.get('interval'))

    interval = 7
    datestr = '20130907'

    date_list = last_week_to_date(datestr, interval)
    domainid = DOMAIN_LIST.index(fieldEnName)

    time_arr = []
    important_arr = []

    for datestr in date_list:
        active, important, reposts, original = getDomainCountData(domainid, datestr)
        if important > 0:
            time_arr.append(ts2date(datetimestr2ts(datestr)).isoformat())
            important_arr.append(important)

    return json.dumps({'time': time_arr, 'important': important_arr})

@mod.route('/group_emotion/<fieldEnName>')
def profile_group_emotion(fieldEnName):
    total_days = 30
    today = datetime.today()
    now_ts = time.mktime(datetime(2013, 10, 1, 2, 0).timetuple())
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
                e_count += xapian_search_weibo.search(query=query_dict, count_only=True)
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
    datestr = '20130901'
    domainid = DOMAIN_LIST.index(fieldEnName)
    verified_count, unverified_count, province_dict = getDomainBasic(domainid, datestr)
    verified_count = int(verified_count)
    unverified_count = int(unverified_count)
    result_list = ''
    if verified_count + unverified_count > 0:
        sumcount = verified_count + unverified_count
        result_list = str(verified_count) + ',' + str(unverified_count) + ',' + str(int(verified_count * 100 / sumcount) / 100.00) + ',' +  str(1 - int(verified_count * 100 / sumcount) / 100.00)

    return json.dumps(result_list)

@mod.route('/group_location/<fieldEnName>')
def profile_group_location(fieldEnName):
    city_count = {}
    datestr = '20130901'
    domainid = DOMAIN_LIST.index(fieldEnName)
    verified_count, unverified_count, province_dict = getDomainBasic(domainid, datestr)
    city_count = province_dict
    results = province_color_map(city_count)
    return json.dumps(results)
