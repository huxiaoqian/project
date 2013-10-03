# -*- coding: utf-8 -*-

import os
import sys
import urllib2
import datetime
from datetime import date
import operator, random
import time
try:
    import simplejosn as json
except ImportError:
    import json

from flask import Blueprint, url_for, render_template, request, abort, flash

from weibo.extensions import db
from weibo.model import *

from utils import hot_uid_by_word, last_week, last_month, ts2date, getFieldUsersByScores, time2ts, datetime2ts
from flask.ext.sqlalchemy import Pagination
import leveldb

from xapian_weibo.xapian_backend import XapianSearch
s_user = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
xapian_search_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)

LEVELDBPATH = '/home/mirage/leveldb'
buckets = {}


def get_bucket(bucket):
    if bucket in buckets:
        return buckets[bucket]
    buckets[bucket] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_' + bucket), block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))    
    return buckets[bucket]

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology']

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

def getUserInfoById(uid):
    query_dict = {
        '_id': int(uid)
    }
    count, get_results = s_user.search(query=query_dict, fields=['created_at', '_id', 'name', \
        'statuses_count', 'followers_count', 'friends_count', 'description', 'profile_image_url', 'verified', 'gender'])
    user = None
    if count > 0:
        for r in get_results():
            statusesCount = r['statuses_count']     #微博数
            followersCount = r['followers_count']   #粉丝数
            friendsCount = r['friends_count']       #关注数
            userName = r['name']                    #昵称   
            description = r['description']          #描述
            uid = r['_id']                          #id
            profileImageUrl = r['profile_image_url']#头像
            verified = r['verified']                #认证 
            gender = r['gender']                    #性别
            user = {'id': uid, 'userName': userName, 'statusesCount': statusesCount, 'followersCount': \
            followersCount, 'friendsCount': friendsCount, 'description': description, 'profileImageUrl': profileImageUrl,
            'verified': verified, 'gender': gender}
            return user
    return user

mod = Blueprint('profile', __name__, url_prefix='/profile')

@mod.route('/search/', methods=['GET', 'POST'])
@mod.route('/search/<model>', methods=['GET', 'POST'])
def profile_search(model='hotest'):
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
            count, get_results = s_user.search(query=query_dict, start_offset=startoffset, max_offset=COUNT_PER_PAGE,
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
            count, get_results = s_user.search(query=query_dict, start_offset=startoffset, max_offset=COUNT_PER_PAGE,
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

@mod.route('/group/<fieldEnName>', methods=['GET', 'POST'])
def profile_group(fieldEnName):
    field = FieldProfile.query.all()
    return render_template('profile/profile_group.html', field=field, model=fieldEnName, atfield=unicode(fieldsEn2Zh(fieldEnName), 'utf-8'))

@mod.route('/person/<uid>', methods=['GET', 'POST'])
def profile_person(uid):
    if uid:
        count, get_results = s_user.search(query={'_id': int(uid)}, fields=['profile_image_url', 'name', 'friends_count', \
                                          'statuses_count', 'followers_count', 'gender', 'verified', 'created_at', 'location'])
        if count > 0:
            for r in get_results():
                user = {'id': uid, 'profile_image_url': r['profile_image_url'], 'userName':  unicode(r['name'], 'utf-8'), 'friends_count': r['friends_count'], \
                        'statuses_count': r['statuses_count'], 'followers_count': r['followers_count'], 'gender': r['gender'], \
                        'verified': r['verified'], 'created_at': r['created_at'], 'location': unicode(r['location'], "utf-8")}
        else:
            return 'no such user'
    return render_template('profile/profile_person.html', user=user)

@mod.route('/yymtest/', methods=['GET', 'POST'])
def yymtest():
    return render_template('Sunburst - Sunburst of a Directory Tree.html')

@mod.route('/person_tab_ajax/<model>/<uid>')
def profile_person_tab_ajax(model, uid):
    if model == 'personaltopic':
        return render_template('profile/ajax/personal_word_cloud.html', uid=uid)
    elif model == 'personalweibocount':
        return render_template('profile/ajax/personal_weibo_count.html', uid=uid)
    elif model == 'personalnetwork':
        return render_template('profile/ajax/personal_network.html', uid=uid)
    elif model == 'personal_friends_followers':       
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
    except KeyError:
        field = ''
    count, get_results = s_user.search(query={'_id': int(user_id)}, fields=['profile_image_url', 'name'])
    name = None
    profile_image_url = None
    if count:
        for r in get_results():
            name = r['name']
            profile_image_url = r['profile_image_url']
    return {"name": name, "group":group, "profile_image_url": profile_image_url, "domain": field, "weight": weight}

@mod.route('/person_network/<uid>', methods=['GET', 'POST'])
def profile_network(uid):
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
        def node(friendsCount,followersCount,statusesCount,gender,verified,profileImageUrl,id,name):
            return {"children":[],"data":{"friendsCount":friendsCount,"followersCount":followersCount,"statusesCount":statusesCount,"gender":gender,"verified":verified,"profileImageUrl":profileImageUrl,"$color":"#AEA9F8","$angularWidth":1000,},"id": id,"name": name}
        def unode(uid,name):
            return {"children":[],"data":{"$color":"#AEA9F8","$angularWidth":1000},"id": uid,"name": name}
            #FCD9A1 AEA9F8 B0AAF6 B2ABF4 B6AEEF E0C7C0 D2BFD0 ECCFB3 D4C0CE	
        def source(uid,name):
            return {"children":[],"data":{"type":"none"},"id": uid,"name": name}
        
        first=source(center_uid ,getUserInfoById(center_uid)['userName'] )
        second=[]
        third=[]
        order=[9,3,3,2,5,5,2,7]
        
        for i in top_8_fri:
            info=getUserInfoById(i)
            if(info==None):
                second.append(unode(i,i))
            else:
                second.append(node(info['friendsCount'],info['followersCount'],info['statusesCount'],info['gender'],info['verified'],info['profileImageUrl'],info['id'],info['userName']))

            
        for i in range(0,8):
            second[i]['data']['$color']="#B2ABF4"
             
        for i in top_36_fri:
            info=getUserInfoById(i)
            if(info==None):
                third.append(unode(i,i))
            else:
                third.append(node(info['friendsCount'],info['followersCount'],info['statusesCount'],info['gender'],info['verified'],info['profileImageUrl'],info['id'],info['userName']))

        sum=0
        for i in range(0,8):
            for k in range(0,order[i]):
                (second[i]['children']).append(third[sum+k])
            sum=sum+order[i]
            
        first['children'] = second
        print first
        return json.dumps(first)
    

# user = {'id': uid, 'userName': userName, 'statusesCount': statusesCount, 'followersCount': followersCount, 'friendsCount': friendsCount, 'description': description, 'profileImageUrl': profileImageUrl,
           # 'verified': verified, 'gender': gender}
    
@mod.route('/person_fri_fol/<uid>', methods=['GET', 'POST'])
def profile_person_fri_fol(uid):
    if request.method == 'GET' and uid and request.args.get('page'):
        page = int(request.args.get('page'))
        COUNT_PER_PAGE = 10
        if page == 1:
            startoffset = 0
        else:
            startoffset = (page - 1) * COUNT_PER_PAGE
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
        field_bucket = get_bucket('user_daily_field')
        user_dict = {}
        field_user_count = {}
        if fri_fol != []:
            for user_id in set(fri_fol[:10000]):
                try:
                    fields = field_bucket.Get(str(user_id) + '_' + '20130430')
                    field = fields.split(',')[0]
                except KeyError:
                    continue
                try:
                    field_user_count[field] = field_user_count[field] + 1
                except KeyError:
                    field_user_count[field] = 1
                count, get_results = s_user.search(query={'_id': user_id}, fields=['_id', 'name', 'statuses_count', 'followers_count', 'friends_count'])
                if count > 0:
                    for user in get_results():
                        user_dict[user_id] = [user['name'], user['statuses_count'], user['followers_count'], user['friends_count'], field]
        sorted_users = sorted(user_dict.items(), key=lambda d: d[1][2], reverse=True)
        result = []
        for id, value in sorted_users:
            result.append({'id': id,'userName': unicode(value[0], "utf-8"), 'statusesCount': value[1],
                           'followersCount': value[2], 'friendsCount': value[3],
                           'field': fieldsEn2Zh(value[4])})
        total_pages = len(result) / COUNT_PER_PAGE + 1
        try:
            users = result[startoffset:(startoffset+COUNT_PER_PAGE-1)]
        except:
            users = result[startoffset: len(result)-1]
        sorted_field_count = sorted(field_user_count.items(), key=lambda d: d[1], reverse=True)
        return json.dumps({'users': users, 'pages': total_pages, 'fields': sorted_field_count})

@mod.route('/person_topic/<uid>', methods=['GET', 'POST'])
def profile_person_topic(uid):
    if request.method == 'GET' and uid:
        from datetime import datetime
        from burst_word import date2ts
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
            lda_topic_bucket = get_bucket('user_lda_topics')
            try:
                topics = json.loads(lda_topic_bucket.Get(str(uid)))
                sortedtopics = sorted(topics.iteritems(), key=operator.itemgetter(1), reverse=True)
                for k, v in sortedtopics[:limit]:
                    result_arr.append({'text': k, 'size': float(v)})
            except KeyError:
                result_arr = []
        if topic_type == 'bst':
            from burst_word import read_burst_wordsFromDb, burst_model, sort_busrt_words
            from utils import last_day
            time_start, time_end = last_day(interval)
            if interval == 1:
                window_size = 3600
            else:
                window_size = 24*60*60
            topics = read_burst_wordsFromDb(uid, time_start, time_end, window_size)
            if topics:
                result_arr = sort_busrt_words(topics, sort=sort, limit=limit)
            else:
                new_topics = burst_model(time_start, time_end, uidnumber=uid, window_size = window_size)
                if new_topics:
                    result_arr = sort_busrt_words(new_topics, sort=sort, limit=limit)
        return json.dumps(result_arr)
    else:
        return json.dumps([])

@mod.route('/person_count/<uid>', methods=['GET', 'POST'])
def personal_weibo_count(uid):
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

    for i in range(total_days-1, -1, -1):
        end_ts = now_ts - i * during
        begin_ts = end_ts - during 

        query_dict = {
            'timestamp': {
                '$gt': begin_ts,
                '$lt': end_ts
            },
            'user': int(uid),
        }

        count, get_results = xapian_search_weibo.search(query=query_dict, fields=['retweeted_status'])
        post_count = 0
        fipost_count = 0
        repost_count = 0
        for r in get_results():
            if r['retweeted_status']:
                repost_count += 1
            else:
                fipost_count += 1
            post_count += 1

        post_arr.append(post_count)
        fipost_arr.append(fipost_count)
        repost_arr.append(repost_count)
        time_arr.append(ts2date(end_ts).isoformat())

    return json.dumps({'time': time_arr, 'count': post_arr, 'repost': repost_arr, 'fipost': fipost_arr})

    '''
    for i in range(total_days-1, -1, -1):
        end_ts = now_ts - i * during 
        begin_ts = end_ts - during 

        fipost_query_dict = {
            'timestamp': {
                '$gt': begin_ts,
                '$lt': end_ts
            },
            'user': int(uid),
            'retweeted_status': None
        }

        repost_query_dict = {
            'timestamp': { 
                '$gt': begin_ts,
                '$lt': end_ts
            },
            'user': int(uid),
            '$not': {'retweeted_status': None}
        }

        fipost_count = xapian_search_weibo.search(query=fipost_query_dict, count_only=True)
        repost_count = xapian_search_weibo.search(query=repost_query_dict, count_only=True)
        total_count = fipost_count + repost_count
        post_arr.append(total_count)
        fipost_arr.append(fipost_count)
        repost_arr.append(repost_count)
        time_arr.append(ts2date(end_ts).isoformat())

    return json.dumps({'time': time_arr, 'count': post_arr, 'repost': repost_arr, 'fipost': fipost_arr})
    '''

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
