# -*- coding: utf-8 -*-

from __future__ import division
import  calendar
import re
from datetime import datetime
import os
import heapq
import time
from datetime import date

from model import *
from config import db


from xapian_weibo.xapian_backend import XapianSearch
from BeautifulSoup import BeautifulSoup
from city_color import province_color_map
from global_config import xapian_search_user as user_search
path = '/home/ubuntu12/dev/data/stub/master_timeline_weibo_'

def ts2datetimestr(ts):
    return time.strftime('%Y%m%d', time.localtime(ts))

def date2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def getXapianWeiboByDuration(datestr_list):
    stub_file_list = []

    for datestr in datestr_list:
        stub_file = path + datestr
        print type(stub_file)
        if os.path.exists(stub_file):
            stub_file_list.append(stub_file)

    if len(stub_file_list):
        xapian_search_weibo = XapianSearch(stub=stub_file_list, include_remote=True, schema_version=5)
        return xapian_search_weibo 

    else:
        return None

def getXapianWeiboByTs(start_time, end_time):
    xapian_date_list =[]
    end_time = end_time + 24*3600
    Day = 24*3600
    days = int((int(end_time) - int(start_time)) / Day)

    for i in range(0, days):
        _ts = start_time + i * Day
        xapian_date_list.append(ts2datetimestr(_ts))
    print xapian_date_list
    statuses_search = getXapianWeiboByDuration(xapian_date_list)
    return statuses_search

class TopkHeap(object):
    def __init__(self, k):
        self.k = k
        self.data = []
 
    def Push(self, elem):
        if len(self.data) < self.k:
            heapq.heappush(self.data, elem)
        else:
            topk_small = self.data[0][0]
            if elem[0] > topk_small:
                heapq.heapreplace(self.data, elem)
 
    def TopK(self):
        return [x for x in reversed([heapq.heappop(self.data) for x in xrange(len(self.data))])]

def get_user(uid):
    user = {}
    count,get_results = user_search.search(query={'_id': uid})
    for r in get_results():
        user['id'] = r['_id']
        user['province'] = r['province']
        user['bi_followers_count'] = 'None'
        user['verified'] = r['verified']
        user['description'] = r['description']
        if not r['friends_count']:
            user['friends_count'] = 0
        else:
            user['friends_count'] = r['friends_count']
        user['city'] = r['city']
        user['gender']  = r['gender']
        user['profile_image_url'] = r['profile_image_url']
        user['verified_reason'] = 'None'
        if not r['followers_count']:
            user['followers_count'] = 0
        else:
            user['followers_count'] = r['followers_count']

        user['location'] = r['location']
        if not r['statuses_count']:
            user['statuses_count'] = 0
        else:
            user['statuses_count'] = r['statuses_count']

        if r['name']:
            user['name'] = r['name']
        else:
            user['name'] = u'未知用户'
        break
    if user == {}:
        return None
    else:
        return user

def get_ori_status(_id, beg_ts, end_ts):
    
    status ={}
    statuses_search = getXapianWeiboByTs(beg_ts,end_ts)
    count,get_results = statuses_search.search(query={'_id': _id},fields=['text','_id','geo','source','retweeted_mid','reposts_count','comments_count','attitudes_count','user','timestamp'])

    for r in get_results():
        status['text'] = r['text']
        status['id'] = r['_id']
        if r['geo']:
            status['geo'] = r['geo']
        else:
            status['geo'] = None
        status['sourcePlatform'] = 'None'
        if r['retweeted_mid']:
            status['retweetedMid'] = r['retweeted_mid']
        else:
            status['retweetedMid'] = None
        if not r['reposts_count']:
            status['repostsCount'] = 0
        else:
            status['repostsCount'] = r['reposts_count']
        if not r['comments_count']:
            status['commentsCount'] = 0
        else:
            status['commentsCount'] = r['comments_count']
        
        if 'attitudes_count' not in r:
            status['attitudesCount'] = 0
        else:
            if not r['attitudes_count']:
                status['attitudesCount'] = 0
            else:
                status['attitudesCount'] = r['attitudes_count']

        if r['user']: 
            status['user'] = get_user(r['user'])
        else:
            status['user'] = {'name':'未知用户'}

        status['timestamp'] =r['timestamp']
        status['postDate'] = datetime.fromtimestamp(r['timestamp'])
        break

    return status
    
def calculate_single(_id, beg_ts,end_ts):#统计含_id的整个树的各指标

    #初始化
    blog_info = {}
    city_count = {}
    topic_rel_blog = dict()
    province_name=dict()
    html = '''<select name="province" id="province" defvalue="11"><option value="34">安徽</option><option value="11">北京</option><option value="50">重庆</option><option value="35">福建</option><option value="62">甘肃</option>
                <option value="44">广东</option><option value="45">广西</option><option value="52">贵州</option><option value="46">海南</option><option value="13">河北</option>
                <option value="23">黑龙江</option><option value="41">河南</option><option value="42">湖北</option><option value="43">湖南</option><option value="15">内蒙古</option><option value="32">江苏</option>
                <option value="36">江西</option><option value="22">吉林</option><option value="21">辽宁</option><option value="64">宁夏</option><option value="63">青海</option><option value="14">山西</option><option value="37">山东</option>
                <option value="31">上海</option><option value="51">四川</option><option value="12">天津</option><option value="54">西藏</option><option value="65">新疆</option><option value="53">云南</option><option value="33">浙江</option>
                <option value="61">陕西</option><option value="71">台湾</option><option value="81">香港</option><option value="82">澳门</option><option value="400">海外</option><option value="100">其他</option></select>'''
    province_soup = BeautifulSoup(html)
    for province in province_soup.findAll('option'):
        pp = province.string
        key = province['value']
        province_name[key] = pp
        if pp == u'海外' or pp == u'其他':
            continue
        city_count[pp] = 0
    
    beg_limit = time.mktime(datetime(2013, 9, 1).timetuple())
    end_limit = time.mktime(datetime(2013, 9, 5).timetuple())
    #获取原微博信息
    status_ori = get_ori_status(_id, beg_ts, end_ts)
    if not status_ori:
        return 'None'
    if status_ori['retweetedMid']:#该微博为转发微博
        time_ts = status_ori['timestamp']
        beg_ts = time_ts - 7*24*3600
        end_ts = time_ts
        if beg_ts < beg_limit:
            beg_ts = beg_limit
        if end_ts > end_limit:
            end_ts = end_limit
        retweeted_ori = get_ori_status(status_ori['retweetedMid'], beg_ts, end_ts)#获取原创微博信息
    else:#该微博为原创微博
        retweeted_ori = status_ori
 
    if retweeted_ori:
        time_ts = retweeted_ori['timestamp']
        beg_ts = time_ts
        end_ts = time_ts + 2*24*3600
        if beg_ts < beg_limit:
            beg_ts = beg_limit
        if end_ts > end_limit:
            end_ts = end_limit
        statuses_search = getXapianWeiboByTs(beg_ts,end_ts)
        count,get_results = statuses_search.search(query={'retweeted_mid': retweeted_ori['id']}, sort_by=['timestamp'])
    else:
        return 'None'
    print count
    reposter = []
    
    date_list = []
    date_list.append(date.fromtimestamp(status_ori['timestamp']))
    perday_repost_count = []
    perday_repost_count.append(1)
    
    per = date(2000,01,02)-date(2000,01,01)
    reposts_sum = 0
    comments_sum = 0
    key_reposter = dict()
    weibo = dict()
    topic_user = dict()
    leader_index = 0
    n = 0 
    for r in get_results():

        if not r['reposts_count']:
            r['reposts_count'] = 0
        if not r['comments_count']:
            r['comments_count'] = 0

        if 'attitudes_count' not in r:
            r['attitudes_count'] = 0
        else:
            if not r['attitudes_count']:
                r['attitudes_count'] = 0

        if r['user']:
            user = get_user(r['user'])
            if not user:
                continue
            if user['province'] != None:
                p = province_name[user['province']]
                if p == u'海外' or p == u'其他':
                    pass
                else:
                    city_count[p] += 1
            if user['id'] not in reposter:
                reposter.append(user['id'])
            if r['reposts_count'] > 1000:
                leader_index = leader_index + 1
            if topic_user.has_key(r['user']):#生成用户对应的排序指标
                pass
            else:
                topic_user[r['user']] = user['friends_count'] + user['followers_count'] + user['statuses_count']
            if key_reposter.has_key(r['user']):#存储用户信息
                pass
            else:                
                key_reposter[r['user']] = user
            temp = {}
            temp['status'] = r
            if not r['reposts_count']:
                temp['status']['repostsCount'] = 0
            else:
                temp['status']['repostsCount'] = r['reposts_count']
            if not r['comments_count']:
                temp['status']['commentsCount'] = 0
            else:
                temp['status']['commentsCount'] = r['comments_count']
            if 'attitudes_count' not in r:
                temp['status']['attitudesCount'] = 0
            else:
                if not r['attitudes_count']:
                    temp['status']['attitudesCount'] = 0
                else:
                    temp['status']['attitudesCount'] = r['attitudes_count']
            temp['status']['created_at'] = datetime.fromtimestamp(r['timestamp'])
            temp['status']['text'] = r['text']
            temp['status']['source'] = 'None'
            temp['user'] = user
            
            if weibo.has_key(temp['status']['_id']):#生成微博对应的排序指标
                pass
            else:
                weibo[temp['status']['_id']] = r['reposts_count']
            if topic_rel_blog.has_key(temp['status']['_id']):#存储微博信息
                pass
            else:
                topic_rel_blog[temp['status']['_id']] = temp
        else:
             continue

        tempdate = date.fromtimestamp(r['timestamp'])
        if tempdate < date_list[-1]:
            if tempdate in date_list:
                temp_index = date_list.index(tempdate)
                perday_repost_count[temp_index] += 1
            else:
                i = 0
                while i < len(date_list):
                    if tempdate > date_list[i] and tempdate < date_list[i+1]:
                        date_list.insert(i+1,tempdate)
                        perday_repost_count.insert(i+1,1)
                        break
                    else:
                        i += 1
        if tempdate == date_list[-1]:
            perday_repost_count[-1] += 1
        if tempdate > date_list[-1]:
            timedelta = date(2000,1,2)-date(2000,1,1)
            while date_list[-1] != tempdate:
                tempdate1 = date_list[-1] + timedelta
                date_list.append(tempdate1)
                perday_repost_count.append(0)
            perday_repost_count[-1] = 1
        reposts_sum += r['reposts_count']
        if 'comments_count' not in r:
            comments_sum += 0
        else:
            comments_sum += r['comments_count']

        n = n + 1
        if n%100 == 0:
            print n

    totalRepost = reposts_sum + 1
    avg = (float(totalRepost))/len(date_list)
    persistent_index = 0
    sudden_count = 0
    j = 0
    while j < len(date_list):
       if perday_repost_count[j] > avg:
          persistent_index += 1
          sudden_count = perday_repost_count[j]-avg+sudden_count
          j += 1
       else:
          j += 1
    sudden_index = '%10.2f'%((float(sudden_count))/totalRepost)
    coverage_index = '%10.2f'%((totalRepost+comments_sum)/float(24*len(date_list)))
   
    media_index = 0
    medias = db.session.query(IMedia).all()
    for media in medias:
        key = media.mediaID
        if key in reposter:
            media_index += 1

    user_th = TopkHeap(1000)
    for d,x in topic_user.items():#用户排序
        user_th.Push((x,d))#排序指标、id

    user_data = user_th.TopK()

    weibo_th = TopkHeap(1000)
    for d,x in weibo.items():#微博排序
        weibo_th.Push((x,d))#排序指标、id

    weibo_data = weibo_th.TopK()
    
    blog_info['status'] = status_ori
    blog_info['user'] = status_ori['user']
    blog_info['datelist'] = date_list
    blog_info['perday_count'] = perday_repost_count
    blog_info['persistent_index'] = persistent_index
    blog_info['sudden_index'] = sudden_index
    blog_info['coverage_index'] = coverage_index
    blog_info['media_index'] = media_index
    blog_info['leader_index'] = leader_index
    blog_info['geo'] = city_count
    blog_info['key_reposter'] = user_data
    blog_info['weibo'] = weibo_data

    save_base_infor(blog_info['status']['id'],blog_info['user']['profile_image_url'],blog_info['status']['text'],blog_info['status']['sourcePlatform'],blog_info['status']['postDate'],blog_info['user']['id'],blog_info['user']['name'],blog_info['status']['repostsCount'],blog_info['status']['commentsCount'],blog_info['status']['attitudesCount'],blog_info['persistent_index'],blog_info['sudden_index'],blog_info['coverage_index'],blog_info['media_index'],blog_info['leader_index'])    

    perday_blog_count = blog_info['perday_count']
    date_list = blog_info['datelist']   
    for i in range(0,len(date_list)):
        save_daily_count(blog_info['status']['id'],date_list[i],perday_blog_count[i])

    for d,x in blog_info['geo'].items():
        save_map(blog_info['status']['id'],d,x)

    for i in range(0,len(blog_info['key_reposter'])):
        uid = blog_info['key_reposter'][i][1]
        save_user(blog_info['status']['id'],uid,key_reposter[uid]['name'],key_reposter[uid]['location'],key_reposter[uid]['followers_count'],key_reposter[uid]['friends_count'],key_reposter[uid]['statuses_count'],key_reposter[uid]['description'],key_reposter[uid]['profile_image_url'])

    if len(blog_info['weibo'])>=5:
        top_weibo = blog_info['weibo'][:5]
    else:
        top_weibo = blog_info['weibo']
    for i in range(0,len(top_weibo)):
        mid = top_weibo[i][1]
        save_weibo(blog_info['status']['id'],mid,topic_rel_blog[mid]['user']['profile_image_url'],topic_rel_blog[mid]['status']['text'],topic_rel_blog[mid]['status']['source'],topic_rel_blog[mid]['status']['created_at'],topic_rel_blog[mid]['user']['id'],topic_rel_blog[mid]['user']['name'],topic_rel_blog[mid]['status']['repostsCount'],topic_rel_blog[mid]['status']['commentsCount'],topic_rel_blog[mid]['status']['attitudesCount'])
  
    return 'Done'

def calculate_part(_id, beg_ts, end_ts, idlist):#统计以_id开头的子树的各指标

    #初始化
    blog_info = {}
    city_count = {}
    topic_rel_blog = dict()
    province_name=dict()
    html = '''<select name="province" id="province" defvalue="11"><option value="34">安徽</option><option value="11">北京</option><option value="50">重庆</option><option value="35">福建</option><option value="62">甘肃</option>
                <option value="44">广东</option><option value="45">广西</option><option value="52">贵州</option><option value="46">海南</option><option value="13">河北</option>
                <option value="23">黑龙江</option><option value="41">河南</option><option value="42">湖北</option><option value="43">湖南</option><option value="15">内蒙古</option><option value="32">江苏</option>
                <option value="36">江西</option><option value="22">吉林</option><option value="21">辽宁</option><option value="64">宁夏</option><option value="63">青海</option><option value="14">山西</option><option value="37">山东</option>
                <option value="31">上海</option><option value="51">四川</option><option value="12">天津</option><option value="54">西藏</option><option value="65">新疆</option><option value="53">云南</option><option value="33">浙江</option>
                <option value="61">陕西</option><option value="71">台湾</option><option value="81">香港</option><option value="82">澳门</option><option value="400">海外</option><option value="100">其他</option></select>'''
    province_soup = BeautifulSoup(html)
    for province in province_soup.findAll('option'):
        pp = province.string
        key = province['value']
        province_name[key] = pp
        if pp == u'海外' or pp == u'其他':
            continue
        city_count[pp] = 0
    
    beg_limit = time.mktime(datetime(2013, 9, 1).timetuple())
    end_limit = time.mktime(datetime(2013, 9, 5).timetuple())
    #获取原微博信息
    status_ori = get_ori_status(_id, beg_ts, end_ts)
    if not status_ori:
        return 'None'
    
    end_ts = beg_ts + 2*24*3600
    if end_ts > end_limit:
        end_ts = end_limit
    reposter = []
    
    date_list = []
    date_list.append(date.fromtimestamp(status_ori['timestamp']))
    perday_repost_count = []
    perday_repost_count.append(1)
    
    per = date(2000,01,02)-date(2000,01,01)
    reposts_sum = 0
    comments_sum = 0
    key_reposter = dict()
    weibo = dict()
    topic_user = dict()
    leader_index = 0
    for rid in idlist:
        r =  get_ori_status(rid, beg_ts, end_ts)
        if not r:
            continue
        if not r['user']:
            continue
        user = r['user']
        if user['province'] != None:
            p = province_name[user['province']]
            if p == u'海外' or p == u'其他':
                pass
            else:
                city_count[p] += 1
        if user['id'] not in reposter:
            reposter.append(user['id'])
        if r['repostsCount'] > 1000:
            leader_index = leader_index + 1
        if topic_user.has_key(user['id']):#生成用户对应的排序指标
            pass
        else:
            topic_user[user['id']] = user['friends_count'] + user['followers_count'] + user['statuses_count']
        if key_reposter.has_key(user['id']):#存储用户信息
            pass
        else:
            key_reposter[user['id']] = user
            
        if weibo.has_key(r['id']):#生成微博对应的排序指标
            pass
        else:
            weibo[r['id']] = r['repostsCount']
        if topic_rel_blog.has_key(r['id']):#存储微博信息
            pass
        else:
            topic_rel_blog[r['id']] = r

        tempdate = date.fromtimestamp(r['timestamp'])
        if tempdate < date_list[-1]:
            if tempdate in date_list:
                temp_index = date_list.index(tempdate)
                perday_repost_count[temp_index] += 1
            else:
                i = 0
                while i < len(date_list):
                    if tempdate > date_list[i] and tempdate < date_list[i+1]:
                        date_list.insert(i+1,tempdate)
                        perday_repost_count.insert(i+1,1)
                        break
                    else:
                        i += 1
        if tempdate == date_list[-1]:
            perday_repost_count[-1] += 1
        if tempdate > date_list[-1]:
            timedelta = date(2000,1,2)-date(2000,1,1)
            while date_list[-1] != tempdate:
                tempdate1 = date_list[-1] + timedelta
                date_list.append(tempdate1)
                perday_repost_count.append(0)
            perday_repost_count[-1] = 1
        reposts_sum += r['repostsCount']
        comments_sum += r['commentsCount']

    totalRepost = reposts_sum + 1
    avg = (float(totalRepost))/len(date_list)
    persistent_index = 0
    sudden_count = 0
    j = 0
    while j < len(date_list):
       if perday_repost_count[j] > avg:
          persistent_index += 1
          sudden_count = perday_repost_count[j]-avg+sudden_count
          j += 1
       else:
          j += 1
    sudden_index = '%10.2f'%((float(sudden_count))/totalRepost)
    coverage_index = '%10.2f'%((totalRepost+comments_sum)/float(24*len(date_list)))
   
    media_index = 0
    medias = db.session.query(IMedia).all()
    for media in medias:
        key = media.mediaID
        if key in reposter:
            media_index += 1

    user_th = TopkHeap(1000)
    for d,x in topic_user.items():#用户排序
        user_th.Push((x,d))#排序指标、id

    user_data = user_th.TopK()

    weibo_th = TopkHeap(1000)
    for d,x in weibo.items():#微博排序
        weibo_th.Push((x,d))#排序指标、id

    weibo_data = weibo_th.TopK()
    
    blog_info['status'] = status_ori
    blog_info['user'] = status_ori['user']
    blog_info['datelist'] = date_list
    blog_info['perday_count'] = perday_repost_count
    blog_info['persistent_index'] = persistent_index
    blog_info['sudden_index'] = sudden_index
    blog_info['coverage_index'] = coverage_index
    blog_info['media_index'] = media_index
    blog_info['leader_index'] = leader_index
    blog_info['geo'] = city_count
    blog_info['key_reposter'] = user_data
    blog_info['weibo'] = weibo_data

    save_base_infor_part(blog_info['status']['id'],blog_info['user']['profile_image_url'],blog_info['status']['text'],blog_info['status']['sourcePlatform'],blog_info['status']['postDate'],blog_info['user']['id'],blog_info['user']['name'],blog_info['status']['repostsCount'],blog_info['status']['commentsCount'],blog_info['status']['attitudesCount'],blog_info['persistent_index'],blog_info['sudden_index'],blog_info['coverage_index'],blog_info['media_index'],blog_info['leader_index'])    

    perday_blog_count = blog_info['perday_count']
    date_list = blog_info['datelist']   
    for i in range(0,len(date_list)):
        save_daily_count_part(blog_info['status']['id'],date_list[i],perday_blog_count[i])

    for d,x in blog_info['geo'].items():
        save_map_part(blog_info['status']['id'],d,x)

    for i in range(0,len(blog_info['key_reposter'])):
        uid = blog_info['key_reposter'][i][1]
        save_user_part(blog_info['status']['id'],uid,key_reposter[uid]['name'],key_reposter[uid]['location'],key_reposter[uid]['followers_count'],key_reposter[uid]['friends_count'],key_reposter[uid]['statuses_count'],key_reposter[uid]['description'],key_reposter[uid]['profile_image_url'])

    if len(blog_info['weibo'])>=5:
        top_weibo = blog_info['weibo'][:5]
    else:
        top_weibo = blog_info['weibo']
    for i in range(0,len(top_weibo)):
        mid = top_weibo[i][1]
        save_weibo_part(blog_info['status']['id'],mid,topic_rel_blog[mid]['user']['profile_image_url'],topic_rel_blog[mid]['text'],topic_rel_blog[mid]['sourcePlatform'],topic_rel_blog[mid]['postDate'],topic_rel_blog[mid]['user']['id'],topic_rel_blog[mid]['user']['name'],topic_rel_blog[mid]['repostsCount'],topic_rel_blog[mid]['commentsCount'],topic_rel_blog[mid]['attitudesCount'])
  
    return 'Done'

def save_base_infor_part(mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount, persistent, sudden, coverage, media, leader):#微博id、头像url、内容、来源、发布时间、用户id、用户昵称、转发数、评论数、赞数、4个指标

    mid = str(mid)
    image_url = str(image_url)
    uid = str(uid)
    repostsCount = int(repostsCount)
    commentsCount = int(commentsCount)
    attitudesCount = int(attitudesCount)
    persistent = float(persistent)
    sudden = float(sudden)
    coverage = float(coverage)
    media = float(media)
    leader = float(leader)

    #print mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount, persistent, sudden, coverage, media, leader
    new_item = PropagateSinglePart(mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount, persistent, sudden, coverage, media, leader)
    db.session.add(new_item)
    db.session.commit()

def save_daily_count_part(mid,date,perday_blog_count):#mid、日期、微博数

    date = ts2datetime(time.mktime(date.timetuple()))
    perday_blog_count = int(perday_blog_count)

    #print mid,date,perday_blog_count
    new_item = PropagateTrendSinglePart(mid,date,perday_blog_count)
    db.session.add(new_item)
    db.session.commit()

def save_map_part(mid,city,count):#mid、城市、数量

    count = int(count)
    #print mid,city,count
    new_item = PropagateSpatialSinglePart(mid,city,count)
    db.session.add(new_item)
    db.session.commit()

def save_user_part(mid,uid,name,location,follower,friend,status,description,profile_image_url):#mid、用户id、用户昵称、地址、粉丝数、关注数、微博数、个人描述

    user = str(uid)
    follower = int(follower)
    friend = int(friend)
    status = int(status)
    if not description:
        description = 'None'
    #print mid,user,name,location,follower,friend,status,description,profile_image_url
    new_item = PropagateUserSinglePart(mid,user,name,location,follower,friend,status,description,profile_image_url)
    db.session.add(new_item)
    db.session.commit()

def save_weibo_part(ori_id,mid,image_url,text,sourcePlatform,postDate,uid,user_name,repostsCount,commentsCount,attitudesCount):#mid、微博

    mid = str(mid)
    image_url = str(image_url)
    uid = str(uid)
    repostsCount = int(repostsCount)
    commentsCount = int(commentsCount)
    attitudesCount = int(attitudesCount)

    #print ori_id,mid,image_url,text,sourcePlatform,postDate,uid,user_name,repostsCount,commentsCount,attitudesCount
    new_item = PropagateWeiboSinglePart(ori_id,mid,image_url,text,sourcePlatform,postDate,uid,user_name,repostsCount,commentsCount,attitudesCount)
    db.session.add(new_item)
    db.session.commit()

def save_base_infor(mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount, persistent, sudden, coverage, media, leader):#微博id、头像url、内容、来源、发布时间、用户id、用户昵称、转发数、评论数、赞数、4个指标

    mid = str(mid)
    image_url = str(image_url)
    uid = str(uid)
    repostsCount = int(repostsCount)
    commentsCount = int(commentsCount)
    attitudesCount = int(attitudesCount)
    persistent = float(persistent)
    sudden = float(sudden)
    coverage = float(coverage)
    media = float(media)
    leader = float(leader)

    #print mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount, persistent, sudden, coverage, media, leader
    new_item = PropagateSingle(mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount, persistent, sudden, coverage, media, leader)
    db.session.add(new_item)
    db.session.commit()

def save_daily_count(mid,date,perday_blog_count):#mid、日期、微博数

    date = ts2datetime(time.mktime(date.timetuple()))
    perday_blog_count = int(perday_blog_count)

    #print mid,date,perday_blog_count
    new_item = PropagateTrendSingle(mid,date,perday_blog_count)
    db.session.add(new_item)
    db.session.commit()

def save_map(mid,city,count):#mid、城市、数量

    count = int(count)
    #print mid,city,count
    new_item = PropagateSpatialSingle(mid,city,count)
    db.session.add(new_item)
    db.session.commit()

def save_user(mid,uid,name,location,follower,friend,status,description,profile_image_url):#mid、用户id、用户昵称、地址、粉丝数、关注数、微博数、个人描述

    user = str(uid)
    follower = int(follower)
    friend = int(friend)
    status = int(status)
    if not description:
        description = 'None'
    #print mid,user,name,location,follower,friend,status,description,profile_image_url
    new_item = PropagateUserSingle(mid,user,name,location,follower,friend,status,description,profile_image_url)
    db.session.add(new_item)
    db.session.commit()

def save_weibo(ori_id,mid,image_url,text,sourcePlatform,postDate,uid,user_name,repostsCount,commentsCount,attitudesCount):#mid、微博

    mid = str(mid)
    image_url = str(image_url)
    uid = str(uid)
    repostsCount = int(repostsCount)
    commentsCount = int(commentsCount)
    attitudesCount = int(attitudesCount)

    #print ori_id,mid,image_url,text,sourcePlatform,postDate,uid,user_name,repostsCount,commentsCount,attitudesCount
    new_item = PropagateWeiboSingle(ori_id,mid,image_url,text,sourcePlatform,postDate,uid,user_name,repostsCount,commentsCount,attitudesCount)
    db.session.add(new_item)
    db.session.commit()

if __name__ == "__main__":

##    topic_info = calculate_part(3617839380294898, 1377964800, 1378224000,[3617782506173763,3618043278635735,3618455003121922,3618481590955662,3618479728507301])
##    print topic_info
    topic_info = calculate_single(3617726042418839, 1377964800, 1378224000)
    print topic_info
