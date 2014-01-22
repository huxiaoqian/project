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
from weibo.model import *
from weibo.extensions import db
from datetime import date, datetime
from city_color import province_color_map
from operator import itemgetter
from flask.ext import admin
from flask import Flask, url_for, render_template, request, make_response, \
                  flash, abort, Blueprint, session, redirect
from utils import acquire_topic_id, weiboinfo2url, getFieldUsersByScores, \
                  getUserNameById, getUserIdByName, merge, \
                  getUsersInfoByUidInteract, user2domain, getFriendship, \
                  yymInfo, _utf_8_decode, getUserInfoById, _utf_8_encode
from time_utils import ts2HMS, last_week_to_date, ts2date, datetimestr2ts, ts2datetime
from weibo.global_config import xapian_search_user, xapian_search_weibo, xapian_search_domain, LEVELDBPATH, \
                                fields_value, fields_id, emotions_zh_kv, emotions_kv, LATEST_DATE, DOMAIN_LIST, \
                                DOMAIN_ZH_LIST
#from _leveldb import getPersonData, getDomainKeywordsData, getDomainBasic, getDomainCountData
from _multi_search import _hotest_users, _newest_users, _domain_users
from _elevator import getPersonData, getDomainKeywordsData, getDomainBasic, getDomainCountData
from _mysql import _search_person_basic, _search_person_important_active, _multi_search

buckets = {}
mod = Blueprint('profile', __name__, url_prefix='/profile')
COUNT_PER_PAGE = 20
month_value = {'January':1, 'February':2, 'March':3, 'April':4, 'May':5, 'June':6, 'July':7, 'August':8, 'September':9, 'October':10, 'November':11, 'December':12}
fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology', 'oversea']
labels = ['university', 'homeadmin', 'abroadadmin', 'homemedia', 'abroadmedia', 'folkorg', \
          'lawyer', 'politician', 'mediaworker', 'activer', 'grassroot', 'other']

#DOMAIN_LIST = fields_value + labels
DOMAIN_LIST = DOMAIN_LIST[9:]
DOMAIN_ZH_LIST = DOMAIN_ZH_LIST[9:]


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
    fields = [{'fieldEnName': f, 'fieldZhName': DOMAIN_ZH_LIST[i]} for i, f in enumerate(DOMAIN_LIST)]
    return statusRange, friendsRange, followersRange, province, fields

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
        statuscount, friendscount, followerscount, province, field = getStaticInfo()
        if session['user'] == 'admin':
            return render_template('profile/index.html', fields=field)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.identify
                    if identy == 1:
                        return render_template('profile/index.html', fields=field)
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

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
                    latest = ts2datetime(datetimestr2ts(LATEST_DATE))
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
                    query_dict['$and'].append({'statuses_count':{'$gt': statusescount_up - 2000000,'$lt': statusescount_up }})
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
                        users = _multi_search(query_dict, sharding)
                    return json.dumps(users[startoffset:endoffset])
        else:
            return redirect('/')
    else:
        return redirect('/')

@mod.route('/group/', methods=['GET','POST'])
def profile_group():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            statuscount, friendscount, followerscount, province, field = getStaticInfo()
            if request.method == 'GET':
                fieldEnName = request.args.get('fieldEnName', None)
                during_time = request.args.get('during_time', None)
                # during_date = _utf_8_encode(during_time)
                # start_ts,end_ts = _time_zone(during_date)
                # window_size = (end_ts - start_ts)/(24*3600)
            return render_template('profile/profile_group.html', field=field, model=fieldEnName, atfield=DOMAIN_ZH_LIST[DOMAIN_LIST.index(fieldEnName)])
        else:
            return redirect('/')
    else:
        return redirect('/')

@mod.route('/person/<uid>', methods=['GET', 'POST'])
def profile_person(uid):
    read_from_xapian = 0
    sharding = False

    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if uid:
                user = {}
                current_time = '20130904'
                active, important, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict = getPersonData(uid, current_time)
                user['active_rank'] = active
                user['important_rank'] = important
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

                return render_template('profile/profile_person.html', user=user)
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
        uid = int(uid)
        center_uid = uid

        direct_uid_interact_count = {}
        retweeted_uid_interact_count = {}
        uid_interact_count = {}

        fri_fol = []
        friends = getFriendship(uid, 'friends')
        followers = getFriendship(uid, 'followers')
        fri_fol.extend(friends)
        fri_fol.extend(followers)

        datestr = '20130907'
        interval = 7
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
            weight_list[0] += third_circle_limit - sum(weight_list)

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
                    for k in range(0, weight_list[i]):
                        second_circle[i]['children'].append(third_circle[start_idx + k])
                    start_idx += weight_list[i]
            
            first_cicle['children'] = second_circle

        return json.dumps({'status': 'finished', 'data': first_cicle})
    
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
                return render_template('profile/ajax/personal_friends.html', uid=uid)
            elif model == 'personalnetwork_follow':       
                return render_template('profile/ajax/personal_followers.html', uid=uid)
            elif model == 'personalinteractnetwork':
                return render_template('profile/ajax/personal_interact.html', uid=uid)
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
                            return render_template('profile/ajax/personal_friends.html', uid=uid)
                        elif model == 'personalnetwork_follow':       
                            return render_template('profile/ajax/personal_followers.html', uid=uid)
                        elif model == 'personalinteractnetwork':
                            return render_template('profile/ajax/personal_interact.html', uid=uid)
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

        fri_fol = getFriendship(uid, friendship)

        direct_uid_interact_count = {}
        retweeted_uid_interact_count = {}
        retweeted_friends_interact_count = {}
        uid_interact_count = {}
        domain_count = {}

        for uid in fri_fol:
            domain = user2domain(uid)
            try:
                domain_count[domain] += 1
            except KeyError:
                domain_count[domain] = 1

        domain_count = [(k, v) for k, v in domain_count.iteritems()]

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
                           'retweeted_uid': retweeted_name_sorted, 'users': users, 'domains': domain_count}})

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

@mod.route('/person_count/<uid>', methods=['GET', 'POST'])
def personal_weibo_count(uid):
    if request.args.get('interval'):
        interval =  int(request.args.get('interval'))

    interval = 7
    datestr = '20130907'

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
        im_arr.append(important)
        emot_arr.append(emoticon)
        time_arr.append(datestr)

    total_post_count = sum(post_arr)
    retweets_count = sum(repost_arr)
    emoticons_count = sum(emot_arr)

    if total_post_count != 0:
        emoticons_ratio = int(emoticons_count * 100 / total_post_count) / 100.0
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
        interval = None
        sort = None
        topic_type = None
        limit = 50
        window_size = 24*60*60
        datestr = '20130901'
        domainid = DOMAIN_LIST.index(fieldEnName) + 9
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
            print result_arr
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
    domainid = DOMAIN_LIST.index(fieldEnName) + 9

    time_arr = []
    total_arr = []
    repost_arr = []
    fipost_arr = []

    for datestr in date_list:
        active, important, reposts, original = getDomainCountData(domainid, datestr)
        sumcount = reposts + original
        #if sumcount > 0:
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
    domainid = DOMAIN_LIST.index(fieldEnName) + 9

    time_arr = []
    important_arr = []

    for datestr in date_list:
        active, important, reposts, original = getDomainCountData(domainid, datestr)
        #if important > 0:
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
    domainid = DOMAIN_LIST.index(fieldEnName) + 9
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
    domainid = DOMAIN_LIST.index(fieldEnName) + 9
    verified_count, unverified_count, province_dict = getDomainBasic(domainid, datestr)
    city_count = province_dict
    results = province_color_map(city_count)
    return json.dumps(results)
