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

from utils import hot_uid_by_word, last_week, last_month, ts2date, getFieldUsersByScores
from flask.ext.sqlalchemy import Pagination
import leveldb

from xapian_weibo.xapian_backend import XapianSearch
s_user = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1) 

LEVELDBPATH = '/home/mirage/leveldb'
buckets = {}
def get_bucket(bucket):
    if bucket in buckets:
        return buckets[bucket]
    buckets[bucket] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_' + bucket), block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))    
    return buckets[bucket]

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology']

COUNT_PER_PAGE = 20

def getStaticInfo():
    statuscount = db.session.query(RangeCount).filter(RangeCount.countType=='statuses').all()
    friendscount = db.session.query(RangeCount).filter(RangeCount.countType=='friends').all()
    followerscount = db.session.query(RangeCount).filter(RangeCount.countType=='followers').all()
    province = db.session.query(Province).order_by(Province.id).all()
    field = db.session.query(FieldProfile).all()
    return statuscount, friendscount, followerscount, province, field

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

            total_days = 6
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
            lowdate, thisdate = last_week()
            uids = hot_uid_by_word(lowdate, thisdate)
            basequery = User.query.filter(User.id.in_(list(uids)))#.order_by(User.followersCount.desc())
            page = int(request.form['page'])
            users = basequery.paginate(page, COUNT_PER_PAGE, False).items
            return json.dumps([i.serialize for i in users])
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
    return render_template('profile/profile_group.html', field=field, model=fieldEnName)

@mod.route('/person/<uid>', methods=['GET', 'POST'])
def profile_person(uid):
    if uid:
        user = User.query.filter_by(id=long(uid)).first_or_404()
    return render_template('profile/profile_person.html', user=user)

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
        result = []
        users = UserField.query.filter_by(fieldFirst='finance').limit(100)
        for user in users:
            result.extend(User.query.filter_by(id=user.uid).limit(20))#.order_by(User.statusesCount.desc()))
        return render_template('profile/ajax/group_rank_bloggers.html', result=result[:9])
    elif model == 'grouplocation':
        return render_template('profile/ajax/group_location.html', field=uid)
    elif model == 'groupactive':
        return render_template('profile/ajax/group_active.html', field=uid)
    elif model == 'groupemotion':
        return render_template('profile/ajax/group_emotion.html', field=uid)

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
                           'field': value[4]})
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
        limit = 100
        window_size = 24*60*60
        if request.args.get('interval') and request.args.get('sort') and request.args.get('limit'):
            interval =  request.args.get('interval')
            sort =  request.args.get('sort')
            limit = int(request.args.get('limit'))
        if interval == 'oneweek':
            lowdate, thisdate = last_week(2, 2)
        else:
            lowdate, thisdate = last_month()
        results = PersonalBurstWords.query.filter(PersonalBurstWords.uid == uid, PersonalBurstWords.windowSize == window_size,
                                                  PersonalBurstWords.startDate>=datetime.fromtimestamp(date2ts(lowdate)),
                                                  PersonalBurstWords.endDate<=datetime.fromtimestamp(date2ts(thisdate))).limit(limit)
        for result in results:
            if sort == 'burst':
                result_arr.append({'text': result.word, 'size': float(result.burst)})
            else:
                result_arr.append({'text': result.word, 'size': result.freq})
        return json.dumps(result_arr)
    else:
        return json.dumps([])

@mod.route('/person_count/<uid>', methods=['GET', 'POST'])
def personal_weibo_count(uid):
    from utils import ts2datetime, time2ts
    from sqlalchemy import func
    result_arr = []
    time_arr = []
    count_arr = []
    repost_arr = []
    fipost_arr = []
    interval = None
    if request.args.get('interval'):
        interval = request.args.get('interval')
    if interval == 'oneweek':
        lowdate, thisdate = last_week(3, 3)
    elif interval == 'onemonth':
        lowdate, thisdate = last_month()
    else:
        lowdate, thisdate = last_week(1, 1)
    startdate = ts2datetime(time2ts(lowdate))
    enddate =  ts2datetime(time2ts(thisdate))
    

    results = db.session.query(func.year(Words.postDate), func.month(Words.postDate), func.day(Words.postDate), func.count(Words.id)).\
              filter(Words.uid==long(uid), Words.postDate>startdate, Words.postDate<enddate).\
              group_by(func.year(Words.postDate), func.month(Words.postDate), func.day(Words.postDate)).all()
    for year, month, day, count in results:
        time_arr.append(date(year, month, day).isoformat())
        count_arr.append(count)
        fi = random.randrange(0, count)
        fipost_arr.append(fi)
        repost_arr.append(count-fi)
    return json.dumps({'time': time_arr, 'count': count_arr, 'repost': repost_arr, 'fipost': fipost_arr})
    '''
    results = db.session.query(Status.postDate, Status.retweetedMid).filter(Status.uid==long(uid), Status.postDate>startdate, Status.postDate<enddate).all()
    print len(results)
    datedict = {}
    for date, retweet in results:
        date = date.date().isoformat()
        isRetweet = 1
        if not retweet or retweet == '':
            isRetweet = 0
        try:
            retCount, firCount = datedict[date]
            if isRetweet:
                retCount += 1
            else:
                firCount += 1
            datedict[date] = retCount, firCount
        except KeyError,e:
            if isRetweet:
                datedict[date] = [1, 0]#[转发数，原创数]
            else:
                datedict[date] = [0, 1]

    sortdatearr = sorted(datedict.iteritems(), key=operator.itemgetter(0), reverse=False)

    for date, count in sortdatearr:
        time_arr.append(date)
        count_arr.append(sum(count))
        repost_arr.append(count[0])
        fipost_arr.append(count[1])
    '''
    

@mod.route('/group_topic/<fieldEnName>')
def profile_group_topic(fieldEnName):
    from datetime import datetime
    from burst_word import date2ts
    result_arr = []
    interval = None
    sort = None
    limit = 100
    window_size = 24*60*60
    s_result = db.session.query(UserField.uid).filter(UserField.fieldFirst == fieldEnName).all()
    uidlist = [uid[0] for uid in s_result]
    if request.args.get('interval') and request.args.get('sort') and request.args.get('limit'):
        interval =  request.args.get('interval')
        sort =  request.args.get('sort')
        limit = int(request.args.get('limit'))
    if interval == 'oneweek':
        lowdate, thisdate = last_week(3, 3)
    else:
        lowdate, thisdate = last_month()

    results = PersonalBurstWords.query.filter(PersonalBurstWords.uid.in_(uidlist), PersonalBurstWords.windowSize == window_size,
                                              PersonalBurstWords.startDate>=datetime.fromtimestamp(date2ts(lowdate)),
                                              PersonalBurstWords.endDate<=datetime.fromtimestamp(date2ts(thisdate))).limit(limit)
    for result in results:
        if sort == 'burst':
            result_arr.append({'text': result.word, 'size': float(result.burst)})
        else:
            result_arr.append({'text': result.word, 'size': result.freq})
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
        interval =  request.args.get('interval')
    if interval == 'oneweek':
        total_days = 6
    elif interval == 'onemonth':
        total_days = 29
    elif interval == 'twomonth':
        total_days = 59
    else:
        total_days = 89

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
    total_days = 6
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600

    interval = None
    if request.args.get('interval'):
        interval =  request.args.get('interval')
    if interval == 'oneweek':
        total_days = 6
    elif interval == 'onemonth':
        total_days = 29
    elif interval == 'twomonth':
        total_days = 59
    else:
        total_days = 89

    bucket = get_bucket('field_daily_active_count')
    uids_set = set([k.split('_')[2] for k in bucket.RangeIter(include_value = False)])
    user_count = {}
    for i in xrange(-total_days + 1, 1):
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
        interval =  request.args.get('interval')
    if interval == 'oneweek':
        total_days = 6
    elif interval == 'onemonth':
        total_days = 29
    elif interval == 'twomonth':
        total_days = 59
    else:
        total_days = 89

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
