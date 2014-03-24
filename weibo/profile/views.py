# -*- coding: utf-8 -*-

try:
    import simplejosn as json
except ImportError:
    import json
import os
import re
import ast
import sys
import time
import json
import leveldb
import urllib2
import operator
from weibo.model import *
from weibo.extensions import db
from datetime import date, datetime
from city_color import province_color_map
from operator import itemgetter
from flask.ext import admin
from flask import Flask, url_for, render_template, request, make_response, \
                  flash, abort, Blueprint, session, redirect, g
from utils import getUserNameById, getUserIdByName, merge, \
                  getUsersInfoByUidInteract, user2domain, getFriendship, \
                  yymInfo, _utf_8_decode, getUserInfoById, _utf_8_encode
from time_utils import ts2HMS, last_week_to_date, ts2date, datetimestr2ts, ts2datetime, ts2datetimestr
from weibo.global_config import xapian_search_user, LEVELDBPATH, \
                                emotions_zh_kv, emotions_kv, LATEST_DATE, DOMAIN_LIST, \
                                DOMAIN_ZH_LIST

from _multi_search import _hotest_users, _newest_users, _domain_users
from _mysql import _search_person_basic, _search_person_important_active, _multi_search
from _ssdb import getPersonData, getDomainKeywordsData, getDomainCountData, getDomainBasic


mod = Blueprint('profile', __name__, url_prefix='/profile')

COUNT_PER_PAGE = 20


def get_default_timerange():
    return u'9月 15日,2013 - 9月 21日,2013'


def get_default_field_dict():
    field_dict = {}
    for idx, field_en in enumerate(DOMAIN_LIST[9:20]):
        field_dict[field_en] = DOMAIN_ZH_LIST[idx+9]

    return field_dict


def get_default_field_name():
    return 'activer', u'活跃人士'


def get_default_hotest_time():
    return ts2datetime(datetimestr2ts(LATEST_DATE) - 24 * 3600)


def get_default_search_time():
    return ts2datetime(datetimestr2ts(LATEST_DATE) - 24 * 3600)


def get_default_profile_person_time():
    return ts2datetimestr(datetimestr2ts(LATEST_DATE) - 24 * 3600)


default_timerange = get_default_timerange()
default_field_dict = get_default_field_dict()
default_field_enname, default_field_zhname = get_default_field_name()
default_profile_person_time = get_default_profile_person_time()


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


def getStaticInfo():
    statuscount = [0, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000]
    friendscount = [0, 400, 800, 1200, 1600, 2000, 2400, 2800, 3200, 3600, 4000]
    followerscount = [0, 6000000, 12000000, 18000000, 24000000, 30000000, 36000000, 42000000, 48000000]
    province = ['北京',  '上海', '香港', '台湾', '重庆', '澳门', '天津', '江苏', '浙江', '四川', '江西', '福建', '青海', '吉林', '贵州', '陕西', '山西', '河北', '湖北', '辽宁', '湖南', '山东', '云南', '河南', '广东', '安徽', '甘肃', '海南', '黑龙江', '内蒙古', '新疆', '广西', '宁夏', '西藏', '海外']
    statusRange = [{'lowBound': statuscount[i], 'upBound': statuscount[i+1]} for i in range(len(statuscount)-1)]
    friendsRange = [{'lowBound': friendscount[i], 'upBound': friendscount[i+1]} for i in range(len(friendscount)-1)]
    followersRange = [{'lowBound': followerscount[i], 'upBound': followerscount[i+1]} for i in range(len(followerscount)-1)]
    province = [{'province': unicode(i, 'utf-8')} for i in province]

    return statusRange, friendsRange, followersRange, province

@mod.route('/log_in', methods=['GET','POST'])
def log_in():
    session['logged_in'] = request.form['log_in']
    session['user'] = request.form['user']
    if 'logged_in' in session and session['logged_in']:
        return json.dumps('Right')
    else:
        return json.dumps('Wrong')

@mod.route('/', methods=['GET','POST'])
def index():
    if 'logged_in' in session and session['logged_in']:
        statuscount, friendscount, followerscount, province = getStaticInfo()
        if session['user'] == 'admin':
            return render_template('profile/index.html', timerange=default_timerange, field_dict=default_field_dict, \
                                   field_zh=default_field_zhname, field_en=default_field_enname)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.identify
                    if identy == 1:
                        return render_template('profile/index.html', timerange=default_timerange, field_dict=default_field_dict, \
                                   field_zh=default_field_zhname, field_en=default_field_enname)
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route('/search/', methods=['GET', 'POST'])
@mod.route('/search/<model>', methods=['GET', 'POST'])
def profile_search(model='hotest'):
    default_search_time = get_default_search_time()
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == 'GET':
                statuscount, friendscount, followerscount, province = getStaticInfo()
                province_str = ''
                province = ['北京',  '上海', '香港', '台湾', '重庆', '澳门', '天津', '江苏', '浙江', '四川', '江西', '福建', '青海', '吉林', '贵州', '陕西', '山西', '河北', '湖北', '辽宁', '湖南', '山东', '云南', '河南', '广东', '安徽', '甘肃', '海南', '黑龙江', '内蒙古', '新疆', '广西', '宁夏', '西藏', '海外'] 
                for pro in province:
                    province_str += unicode(pro, 'utf-8') + ','
                if model == 'person':
                    nickname = urllib2.unquote(request.args.get('nickname'))
                    return render_template('profile/profile_search.html',statuscount=statuscount, \
                                           friendscount=friendscount, followerscount=followerscount, \
                                           location=province_str, model=model, result=None, nickname=nickname, \
                                           field_dict=default_field_dict, time=default_search_time)
                elif model == 'find':
                    statuses_count_upBound = request.args.get('statuses_count_upBound',None)
                    friends_count_upBound = request.args.get('friends_count_upBound',None)
                    followers_count_upBound = request.args.get('followers_count_upBound',None)
                    top_n = request.args.get('search_top_n',None)
                    province_str = request.args.get('province_str',None)
                    rank_str = request.args.get('rankcount',None)
                    rank_str_zh = ''
                    if rank_str == 'followers_count':
                        rank_str_zh = u'粉丝数降序'
                    elif rank_str == 'statuses_count':
                        rank_str_zh = u'微博数降序'
                    elif rank_str == 'friends_count':
                        rank_str_zh = u'关注数降序'
                    elif rank_str == 'created_at':
                        rank_str_zh = u'注册时间降序'
                    if(top_n):
                        result_count = int(top_n)
                    else:
                        result_count = 100
                    if(statuses_count_upBound):
                        statusescount_up = (int(statuses_count_upBound))*10000
                    else:
                        statusescount_up = 100000

                    if(friends_count_upBound):
                        friendscount_up = int(friends_count_upBound)
                    else:
                        friendscount_up = 400

                    if(followers_count_upBound):
                        followerscount_up = (int(followers_count_upBound))*10000
                    else:
                        followerscount_up = 6000000

                    return render_template('profile/profile_search.html',result_count = result_count, statusescount_up = statusescount_up, \
                                           friendscount_up = friendscount_up,followerscount_up = followerscount_up, \
                                           location = province_str, model = model,rankcount=rank_str, field_dict=default_field_dict, \
                                           time=default_search_time, sort=rank_str_zh)  
                else:
                    return render_template('profile/profile_search.html',statuscount=statuscount, \
                                           friendscount=friendscount, followerscount=followerscount, \
                                           location=province_str, model=model, result=None, \
                                           field_dict=default_field_dict, time=default_search_time)
            if request.method == 'POST' and request.form['page']:
                if model == 'newest':
                    top_n = 1000
                    page = int(request.form['page'])
                    if page == 1:
                        startoffset = 0
                    else:
                        startoffset = (page - 1) * COUNT_PER_PAGE
                    endoffset = startoffset + COUNT_PER_PAGE - 1
                    users = _newest_users(top_n)
                    return json.dumps(users[startoffset:endoffset])

                elif model == 'hotest':
                    top_n = 1000
                    latest = get_default_hotest_time()
                    page = int(request.form['page'])
                    if page == 1:
                        startoffset = 0
                    else:
                        startoffset = (page - 1) * COUNT_PER_PAGE
                    endoffset = startoffset + COUNT_PER_PAGE - 1
                    users = _hotest_users(top_n, latest)
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

                elif model in DOMAIN_LIST:
                    page = int(request.form['page'])
                    if page == 1:
                        startoffset = 0
                    else:
                        startoffset = (page - 1) * COUNT_PER_PAGE
                    endoffset = startoffset + COUNT_PER_PAGE - 1

                    users = _domain_users(model)
                    return json.dumps(users[startoffset:endoffset])

                elif model == 'person':
                    sharding = True
                    nickname = urllib2.unquote(request.form['nickname'])
                    uid = getUidByName(nickname)
                    users = []
                    if uid:
                        status, user = _search_person_basic(uid, sharding)
                        if status == 'success':
                            users.append(user._to_dict())                    
                    return json.dumps(users)

                elif model == 'find':
                    read_from_xapian = 0
                    sharding = True

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
                    users = []

                    query_dict = {}
                    query_dict['$and'] = []
                    query_dict['$and'].append({'statuses_count':{'$gt': statusescount_up - 10000,'$lt': statusescount_up }})
                    query_dict['$and'].append({'followers_count':{'$gt': followerscount_up - 6000000 ,'$lt': followerscount_up}})
                    query_dict['$and'].append({'friends_count':{'$gt': friendscount_up - 400 ,'$lt': friendscount_up}})
                    or_dict = {}
                    or_dict['$or'] = []
                    for pro in province:
                        or_dict['$or'].append({'location': pro})
                    query_dict['$and'].append(or_dict)

                    if read_from_xapian:
                        
                        count, get_results = xapian_search_user.search(query=query_dict, max_offset=result_count,
                                                                       fields=[ '_id', 'name', 'statuses_count', 'followers_count', 'friends_count', 'description', 'profile_image_url'], 
                                                                       sort_by=[rankcount])
                        
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
                    else:
                        users = _multi_search(query_dict, rankcount, result_count, sharding)
                    return json.dumps(users[startoffset:endoffset])
        else:
            return redirect('/')
    else:
        return redirect('/')

@mod.route('/group/', methods=['GET','POST'])
def profile_group():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            statuscount, friendscount, followerscount, province = getStaticInfo()
            if request.method == 'GET':
                fieldEnName = request.args.get('fieldEnName', None)
                during_time = request.args.get('during_time', None)
                if during_time:
                    during_date = _utf_8_encode(during_time)
                    start_ts, end_ts = _time_zone(_utf_encode(during_date))
                else:
                    during_time = default_timerange
                    start_ts, end_ts = _time_zone(_utf_encode(during_time))

            return render_template('profile/profile_group.html', model=fieldEnName, \
                                   atfield=default_field_dict[fieldEnName], field_dict=default_field_dict, \
                                   start_ts=start_ts, end_ts=end_ts, field_zh=default_field_dict[fieldEnName], \
                                   field_en=fieldEnName, timerange=during_time)
        else:
            return redirect('/')
    else:
        return redirect('/')


@mod.route('/person.json', methods=['GET', 'POST'])
def profile_person_json():
    nickname = request.args.get('nickname', None)
    if nickname:
        uid = getUidByName(urllib2.unquote(nickname.strip()))
        if uid:
            return json.dumps({'status': 'success', 'uid': str(uid)})

    return json.dumps({'status': 'failed'})


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


@mod.route('/person/<uid>', methods=['GET', 'POST'])
def profile_person(uid):
    read_from_xapian = 0
    sharding = False
    timerange = request.args.get('during_time', None)
    if not timerange:
        timerange = default_timerange
    else:
        timerange = timerange.strip()

    start_ts, end_ts = _time_zone(_utf_encode(timerange))

    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if uid:
                user = {}

                _active, _important, _reposts, _original, _emoticon, _direct_interact, _retweeted_interact, _keywords_dict = getPersonData(uid, default_profile_person_time)
                user['updatetime'] = default_profile_person_time
                user['active_rank'] = _active
                user['important_rank'] = _important

                if read_from_xapian:
                    count, get_results = xapian_search_user.search(query={'_id': int(uid)}, fields=['profile_image_url', 'name', 'friends_count', \
                                                      'statuses_count', 'followers_count', 'gender', 'verified', 'created_at', 'location'])
                    if count > 0:
                        for r in get_results():
                            user_character = {'id': uid, 'profile_image_url': r['profile_image_url'], 'userName':  _utf_8_decode(r['name']), 'friends_count': r['friends_count'], \
                                    'statuses_count': r['statuses_count'], 'followers_count': r['followers_count'], 'gender': r['gender'], \
                                    'verified': r['verified'], 'created_at': r['created_at'], 'location': _utf_8_decode(r['location'])}
                            user.update(user_character)
                            user['created_at'] = ts2HMS(user['created_at']);
                            
                    else:
                        return 'no such user'

                else:
                    status1, personbasic = _search_person_basic(uid, sharding)

                    if status1 == 'success':
                        verifiedTypenum = personbasic.verifiedType
                        friendsCount = personbasic.friendsCount
                        followersCount = personbasic.followersCount
                        statuseCount = personbasic.statuseCount
                        created_at = time.strftime("%m月 %d日, %Y", time.localtime(personbasic.created_at))
                        user_character = {'id': personbasic.userId, 'profile_image_url': personbasic.profileImageUrl, 'userName':  _utf_8_decode(personbasic.name), \
                                'friends_count': friendsCount, 'statuses_count': statuseCount, 'followers_count': followersCount, \
                                'gender': personbasic.gender, 'verified': personbasic.verified, 'created_at': _utf_8_decode(created_at), \
                                'location': _utf_8_decode(personbasic.location), 'date': personbasic.date, \
                                'verifiedTypenum': verifiedTypenum, 'description': _utf_8_decode(personbasic.description)}
                        user.update(user_character)
                    else:
                        return 'no such user'

                return render_template('profile/profile_person.html', user=user, start_ts=start_ts, end_ts=end_ts, timerange=timerange)
            else:
                return redirect('/')
    else:
        return redirect('/')


def getUidByName(name):
    count, users = xapian_search_user.search(query={'name': name}, fields=['_id'])
    if count:
        for user in users():
            return user['_id']
    else:
        return None


@mod.route('/person_interact_network/<uid>', methods=['GET', 'POST'])
def profile_interact_network(uid):
    if request.method == 'GET':
        start_ts = request.args.get('start_ts', None)
        end_ts = request.args.get('end_ts', None)

        if not start_ts or not end_ts:
            start_ts, end_ts = _time_zone(_utf_encode(default_timerange))

        if start_ts:
            start_ts = int(start_ts)

        if end_ts:
            end_ts = int(end_ts)

        uid = int(uid)
        center_uid = uid

        direct_uid_interact_count = {}
        retweeted_uid_interact_count = {}
        uid_interact_count = {}

        try:
            interval = (end_ts - start_ts) / (24 * 3600) + 1
            datestr = ts2datetimestr(end_ts) # '20130907'
        except Exception, e:
            print e
            interval, datestr = _default_time()

        date_list = last_week_to_date(datestr, interval)
    
        for datestr in date_list:
            active, important, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict = getPersonData(uid, datestr)
            for k, v in retweeted_interact.iteritems():
                k = int(k)
                v = int(v)
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
        retweeted_uid2name_dict = {}

        for k, v in retweeted_uid_interact_count.iteritems():
            retweeted_uid2name_dict[getUserNameById(k)] = v

        uid_interact_count = merge(direct_uid_interact_count, retweeted_uid2name_dict, lambda x, y: x+y)
        uid_sorted = sorted(uid_interact_count.iteritems(), key=operator.itemgetter(1), reverse=False)

        second_circle_limit = 8
        third_circle_limit = 36

        second_circle_uid_counts = uid_sorted[-second_circle_limit:]
        third_circle_uid_counts = uid_sorted[-second_circle_limit-third_circle_limit-1:-second_circle_limit-1]

        third_circle_limit = len(third_circle_uid_counts) if len(third_circle_uid_counts) < third_circle_limit else third_circle_limit
        
        def node(friendsCount, followersCount, statusesCount, gender, verified, profileImageUrl, count, uid, name):
            return {
                "children": [],
                "data": {
                    "friendsCount": friendsCount,
                    "followersCount": followersCount,
                    "statusesCount": statusesCount,
                    "gender": gender,
                    "verified": verified,
                    "profileImageUrl": profileImageUrl,
                    "$color": "#AEA9F8",
                    "$angularWidth": 1000,
                    "count": count
                },
                "id": uid,
                "name": name
            }
        
        def unode(uid, name, count):
            return {
                "children": [],
                "data": {
                    "$color": "#AEA9F8", #FCD9A1 AEA9F8 B0AAF6 B2ABF4 B6AEEF E0C7C0 D2BFD0 ECCFB3 D4C0CE
                    "$angularWidth": 1000, 
                    "count":count
                },
                "id": uid,
                "name": name
            }

        def source_data_structure(uid, name):
            return {
                "children": [],
                "data":{
                    "type": "none"
                },
                "id": uid,
                "name": name
            }
        
        # first circle
        first_cicle = source_data_structure(center_uid, getUserNameById(center_uid))
        second_circle = []
        third_circle = []
        
        # calcuate weight list
        second_circle_sum_counts = 0
        for uid, count in second_circle_uid_counts:
            second_circle_sum_counts += count

        # adjust residual weight to index 0 of list
        if second_circle_sum_counts:
            weight_list = [int(count * third_circle_limit / second_circle_sum_counts) if int(count * third_circle_limit / second_circle_sum_counts) >= 1 else 1 for uid, count in second_circle_uid_counts]
            weight_list[0] += second_circle_sum_counts - sum(weight_list)

            # second circle
            for uid, count in second_circle_uid_counts:
                info = yymInfo(uid)
                if not info: 
                    second_circle.append(unode(uid, uid, count))
                else:
                    second_circle.append(node(info['friendsCount'], info['followersCount'], \
                                              info['statusesCount'], info['gender'], \
                                              info['verified'], info['profileImageUrl'], \
                                              count, info['id'], info['userName']))

            # set second circle color
            for i in range(0, len(second_circle)):
                second_circle[i]['data']['$color']="#B2ABF4"            
        
            # third circle   
            for uid, count in third_circle_uid_counts:
                info = yymInfo(uid)
                if not info:
                    third_circle.append(unode(uid, uid, count))
                else:
                    third_circle.append(node(info['friendsCount'], info['followersCount'], \
                                             info['statusesCount'], info['gender'], \
                                             info['verified'], info['profileImageUrl'], \
                                             count, info['id'], info['userName']))
            
            # assign third circle to second circle, then second circle to first circle
            start_idx = 0
            if len(third_circle):
                for i in range(0, len(second_circle)):
                    #for k in range(0, weight_list[i]):
                    if i < len(third_circle):
                        second_circle[i]['children'].append(third_circle[i])
            
            first_cicle['children'] = second_circle

        return json.dumps({'status': 'finished', 'data': first_cicle})
    
@mod.route('/person_tab_ajax/<model>/<uid>')
def profile_person_tab_ajax(model, uid):
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            start_ts = request.args.get('start_ts', None)
            end_ts = request.args.get('end_ts', None)

            if not start_ts or not end_ts:
                start_ts, end_ts = _time_zone(_utf_encode(default_timerange))

            if model == 'personaltopic':
                domain = _utf_decode(user2domain(uid))
                return render_template('profile/ajax/personal_word_cloud.html', uid=uid, fields=domain, start_ts=start_ts, end_ts=end_ts)
            elif model == 'personalweibocount':
                return render_template('profile/ajax/personal_weibo_count.html', uid=uid, start_ts=start_ts, end_ts=end_ts)
            elif model == 'personalnetwork':
                return render_template('profile/ajax/personal_friends.html', uid=uid, start_ts=start_ts, end_ts=end_ts)
            elif model == 'personalnetwork_follow':       
                return render_template('profile/ajax/personal_followers.html', uid=uid, start_ts=start_ts, end_ts=end_ts)
            elif model == 'personalinteractnetwork':
                return render_template('profile/ajax/personal_interact.html', uid=uid, start_ts=start_ts, end_ts=end_ts)
            elif model == 'grouptopic':
                return render_template('profile/ajax/group_word_cloud.html', field=uid, start_ts=start_ts, end_ts=end_ts)
            elif model == 'groupweibocount':
                return render_template('profile/ajax/group_weibo_count.html', field=uid, start_ts=start_ts, end_ts=end_ts)
            elif model == 'grouplocation':
                return render_template('profile/ajax/group_location.html', field=uid, start_ts=start_ts, end_ts=end_ts)
            elif model == 'groupimportant':
                return render_template('profile/ajax/group_important.html', field=uid, start_ts=start_ts, end_ts=end_ts)
        else:
            return redirect('/')
    else:
        return redirect('/')

@mod.route('/person_topic/<uid>', methods=['GET', 'POST'])
def profile_person_topic(uid):
    if request.method == 'GET' and uid:
        result_arr = []
        limit = 50

        start_ts = request.args.get('start_ts', None)
        end_ts = request.args.get('end_ts', None)

        if not start_ts or not end_ts:
            start_ts, end_ts = _time_zone(_utf_encode(default_timerange))

        if start_ts:
            start_ts = int(start_ts)

        if end_ts:
            end_ts = int(end_ts)
        
        try:
            interval = (end_ts - start_ts) / (24 * 3600) + 1
            datestr = ts2datetimestr(end_ts) # '20130907'
        except Exception, e:
            print e
            interval, datestr = _default_time()

        date_list = last_week_to_date(datestr, interval)
        active = important = reposts = original = emoticon = 0
        keywords_dict = {}
        for current_time in date_list:
            _active, _important, _reposts, _original, _emoticon, _direct_interact, _retweeted_interact, _keywords_dict = getPersonData(uid, current_time)
            active += _active
            important += _important
            reposts += _reposts
            original += _original
            emoticon += _emoticon
            if _keywords_dict:
                keywords_dict.update(_keywords_dict)

        if keywords_dict:
            sortedkeywords = sorted(keywords_dict.iteritems(), key=operator.itemgetter(1), reverse=True)
            for k, v in sortedkeywords[:limit]:
                result_arr.append({'text': k, 'size': float(v)})
        
        return json.dumps({'status': 'current finished', 'data': result_arr})

    else:
        return json.dumps([])
    return json.dumps(result_arr)

@mod.route('/person_interact_count', methods=['GET', 'POST'])
def profile_interact_count():
    if request.method == 'GET':
        limit = 10
        start_ts = request.args.get('start_ts', None)
        end_ts = request.args.get('end_ts', None)

        if not start_ts or not end_ts:
            start_ts, end_ts = _time_zone(_utf_encode(default_timerange))

        if start_ts:
            start_ts = int(start_ts)

        if end_ts:
            end_ts = int(end_ts)

        direct_uid_interact_count = {}
        retweeted_uid_interact_count = {}
        uid_interact_count = {}
        domain_count = {}

        try:
            interval = (end_ts - start_ts) / (24 * 3600) + 1
            datestr = ts2datetimestr(end_ts) # '20130907'
        except Exception, e:
            print e
            interval, datestr = _default_time()

        date_list = last_week_to_date(datestr, interval)
    
        for datestr in date_list:
            active, important, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict = getPersonData(uid, datestr)
            for k, v in retweeted_interact.iteritems():
                k = int(k)
                v = int(v)

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
        retweeted_uid2name_dict = {}

        for k, v in retweeted_uid_interact_count.iteritems():
            retweeted_uid2name_dict[getUserNameById(k)] = v
        uid_interact_count = merge(direct_uid_interact_count, retweeted_uid2name_dict, lambda x, y: x+y)
        uid_sorted = sorted(uid_interact_count.iteritems(), key=operator.itemgetter(1), reverse=True)

        direct_uid_sorted = direct_uid_sorted[-limit:]
        retweeted_uid_sorted = retweeted_uid_sorted[-limit:]

        retweeted_name_sorted = []
        for uid, count in retweeted_uid_sorted:
            retweeted_name_sorted.append((getUserNameById(uid), count))
        
        return json.dumps({'status': 'finished', 'data': {'direct_uid': direct_uid_sorted, \
                           'retweeted_uid': retweeted_name_sorted}})

@mod.route('/person_network/<friendship>/<uid>', methods=['GET', 'POST'])
def profile_network(friendship, uid):
    if request.method == 'GET':
        start_ts = request.args.get('start_ts', None)
        end_ts = request.args.get('end_ts', None)

        if not start_ts or not end_ts:
            start_ts, end_ts = _time_zone(_utf_encode(default_timerange))

        if start_ts:
            start_ts = int(start_ts)

        if end_ts:
            end_ts = int(end_ts)

        limit = 10
        fri_fol = getFriendship(uid, friendship)
        fri_fol_interact_uid_count = {}
        domain_count = {}
        
        # 关注者、粉丝领域统计
        for uid in fri_fol:
            domain = user2domain(uid)
            try:
                domain_count[domain] += 1
            except KeyError:
                domain_count[domain] = 1

        domain_count = [(k, v) for k, v in domain_count.iteritems()]
        
        try:
            interval = (end_ts - start_ts) / (24 * 3600) + 1
            datestr = ts2datetimestr(end_ts) # '20130907'
        except Exception, e:
            print e
            interval, datestr = _default_time()

        date_list = last_week_to_date(datestr, interval)
    
        for datestr in date_list:
            active, important, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict = getPersonData(uid, datestr)
            for k, v in retweeted_interact.iteritems():
                k = int(k)
                v = int(v)
                if k in set(fri_fol):
                    try:
                        fri_fol_interact_uid_count[k] += v
                    except KeyError:
                        fri_fol_interact_uid_count[k] = v

            for k, v in direct_interact.iteritems():
                v = int(v)
                k = getUidByName(k)
                if k and k in set(fri_fol):
                    try:
                        fri_fol_interact_uid_count[k] += v
                    except KeyError:
                        fri_fol_interact_uid_count[k] = v
        
        fri_fol_interact_name_count = {}
        for k, v in fri_fol_interact_uid_count.iteritems():
            name = getUserNameById(k)
            fri_fol_interact_name_count[name] = v

        fri_fol_interact_name_sorted = sorted(fri_fol_interact_name_count.iteritems(), key=operator.itemgetter(1), reverse=True)
        sorted_uid_interact_count = sorted(fri_fol_interact_uid_count.iteritems(), key=operator.itemgetter(1), reverse=True)
        users = getUsersInfoByUidInteract(sorted_uid_interact_count)

        return json.dumps({'status': 'finished', 'data': {'fri_fol_interact': fri_fol_interact_name_sorted, \
                           'users': users, 'domains': domain_count}})

@mod.route('/person_count/<uid>', methods=['GET', 'POST'])
def personal_weibo_count(uid):
    start_ts = request.args.get('start_ts', None)
    end_ts = request.args.get('end_ts', None)
    if not start_ts or not end_ts:
        start_ts, end_ts = _time_zone(_utf_encode(default_timerange))

    if start_ts:
        start_ts = int(start_ts)
    if end_ts:
        end_ts = int(end_ts)

    try:
        interval = (end_ts - start_ts) / (24 * 3600) + 1
        datestr = ts2datetimestr(end_ts) # '20130907'
    except Exception, e:
        print e
        interval, datestr = _default_time()

    date_list = last_week_to_date(datestr, interval)

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
        print important
        im_arr.append(important)
        emot_arr.append(emoticon)
        time_arr.append(datestr)

    total_post_count = sum(post_arr)
    retweets_count = sum(repost_arr)
    emoticons_count = sum(emot_arr)

    if total_post_count != 0:
        emoticons_ratio = int(emoticons_count * 100 / total_post_count) / 100.000
        retweetes_ratio = int(retweets_count * 100 / total_post_count) / 100.0
    else:
        emoticons_ratio = 0.0
        retweetes_ratio = 0.0

    return json.dumps({'time': time_arr, 'count': post_arr, 'repost': repost_arr, 'fipost': fipost_arr, \
                       'important': im_arr, 'total_tweets': total_post_count, \
                       'retweets_ratio': retweetes_ratio, 'emoticons_ratio': emoticons_ratio})


@mod.route('/group_topic/<fieldEnName>')
def profile_group_topic(fieldEnName):
    if request.method == 'GET' and fieldEnName:
        result_arr = []

        sort = request.args.get('sort', 'prob')
        topic_type = request.args.get('topic_type', 'freq')
        limit = request.args.get('limit', 50)
        if limit:
            limit = int(limit)
        window_size = request.args.get('window_size', 24 * 60 * 60)
        if window_size:
            window_size = int(window_size)
        
        start_ts = request.args.get('start_ts', None)
        end_ts = request.args.get('end_ts', None)

        if not start_ts or not end_ts:
            start_ts, end_ts = _time_zone(_utf_encode(default_timerange))

        if start_ts:
            start_ts = int(start_ts)

        if end_ts:
            end_ts = int(end_ts)
        
        interval = (end_ts - start_ts) / (24 * 3600) + 1
        datestr = ts2datetimestr(end_ts) # '20130907'

        domainid = DOMAIN_LIST.index(fieldEnName)
        date_list = last_week_to_date(datestr, interval)

        keywords_dict = {}
        for datestr in date_list:
            _keywords_dict = getDomainKeywordsData(domainid, datestr)
            for k, v in _keywords_dict.iteritems():
                try:
                    keywords_dict[k] += v
                except KeyError:
                    keywords_dict[k] = v

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
    start_ts = request.args.get('start_ts', None)
    end_ts = request.args.get('end_ts', None)

    if not start_ts or not end_ts:
        start_ts, end_ts = _time_zone(_utf_encode(default_timerange))

    if start_ts:
        start_ts = int(start_ts)

    if end_ts:
        end_ts = int(end_ts)
    
    interval = (end_ts - start_ts) / (24 * 3600) + 1
    datestr = ts2datetimestr(end_ts) # '20130907'

    date_list = last_week_to_date(datestr, interval)
    domainid = DOMAIN_LIST.index(fieldEnName)

    time_arr = []
    total_arr = []
    repost_arr = []
    fipost_arr = []

    for datestr in date_list:
        active, important, reposts, original = getDomainCountData(domainid, datestr)
        sumcount = reposts + original
        time_arr.append(ts2date(datetimestr2ts(datestr)).isoformat())
        total_arr.append(sumcount)
        repost_arr.append(reposts)
        fipost_arr.append(original)

    return json.dumps({'time': time_arr, 'count': total_arr, 'repost': repost_arr, 'fipost': fipost_arr})


def _default_time():
    interval = 7
    datestr = LATEST_DATE
    return interval, datestr

@mod.route('/group_important/<fieldEnName>', methods=['GET', 'POST'])
def group_important_count(fieldEnName):
    start_ts = request.args.get('start_ts', None)
    end_ts = request.args.get('end_ts', None)

    if not start_ts or not end_ts:
        start_ts, end_ts = _time_zone(_utf_encode(default_timerange))

    if start_ts:
        start_ts = int(start_ts)

    if end_ts:
        end_ts = int(end_ts)
    
    interval = (end_ts - start_ts) / (24 * 3600) + 1
    datestr = ts2datetimestr(end_ts) # '20130907'

    date_list = last_week_to_date(datestr, interval)
    domainid = DOMAIN_LIST.index(fieldEnName)

    time_arr = []
    important_arr = []

    for datestr in date_list:
        active, important, reposts, original = getDomainCountData(domainid, datestr)
        #if important > 0:
        time_arr.append(ts2date(datetimestr2ts(datestr)).isoformat())
        important_arr.append(important)

    return json.dumps({'time': time_arr, 'important': important_arr})


@mod.route('/group_verify/<fieldEnName>')
def profile_group_verify(fieldEnName):
    domainid = DOMAIN_LIST.index(fieldEnName)
    datestr = '20130901'

    verified_count, unverified_count, province_dict = 0, 0, {}
    _verified_count, _unverified_count, _province_dict = getDomainBasic(domainid, datestr)
    verified_count = int(_verified_count)
    unverified_count = int(_unverified_count)
    province_dict = _province_dict

    result_list = ''
    if verified_count + unverified_count > 0:
        sumcount = verified_count + unverified_count
        result_list = str(verified_count) + ',' + str(unverified_count) + ',' + str(int(verified_count * 100 / sumcount) / 100.00) + ',' +  str(1 - int(verified_count * 100 / sumcount) / 100.00)

    return json.dumps(result_list)

@mod.route('/group_location/<fieldEnName>')
def profile_group_location(fieldEnName):
    domainid = DOMAIN_LIST.index(fieldEnName)
    datestr = '20130901'

    verified_count, unverified_count, province_dict = 0, 0, {}
    _verified_count, _unverified_count, _province_dict = getDomainBasic(domainid, datestr)
    verified_count = int(_verified_count)
    unverified_count = int(_unverified_count)
    province_dict = _province_dict

    city_count = province_dict
    results = province_color_map(city_count)
    return json.dumps(results)
