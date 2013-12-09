# -*- coding: utf-8 -*-

from __future__ import division
import  calendar
import re
from datetime import datetime

import time
from datetime import date

from weibo.model import *
from weibo.extensions import db


from xapian_weibo.xapian_backend import XapianSearch
from BeautifulSoup import BeautifulSoup
from city_color import province_color_map
from xapian_config import xapian_search_user as s
from xapian_config import xapian_search_weibo as s_weibo

def get_user(uid):
    user = {}
    #s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user')
    count,get_results = s.search(query={'_id': uid},fields = ['_id','province','bi_followers_count','verified','description','friends_count','city','gender','created_at','profile_image_url','verified_reason','followers_count','location','name','active','statuses_count'])
    for r in get_results():
        user['id'] = r['_id']
        user['province'] = r['province']
        user['bi_followers_count'] = r['bi_followers_count']
        user['verified'] = r['verified']
        user['description'] = r['description']
        user['friends_count'] = r['friends_count']
        user['city'] = r['city']
        user['gender']  = r['gender']
        user['created_at'] = r['created_at']
        user['profile_image_url'] = r['profile_image_url']
        #user['verified_reason'] = r['verified_reason'].decode("utf-8")
        user['followers_count'] = r['followers_count']
        user['location'] = r['location']
        user['active'] = r['active']
        user['statuses_count'] = r['statuses_count']
        user['name'] = r['name']
        user['userField'] = u'未知领域'
        break
    if user == {}:
        user['id'] = None
        user['province'] = None
        user['bi_followers_count'] = None
        user['verified'] = None
        user['description'] = None
        user['friends_count'] = None
        user['city'] = None
        user['gender']  = None
        user['created_at'] = None
        user['profile_image_url'] = None
        #user['verified_reason'] = None
        user['followers_count'] = None
        user['location'] = None
        user['active'] = None
        user['statuses_count'] = None
        user['name'] = u'未知用户'
        user['userField'] = u'未知领域'
        return user
    else:
        return user

def get_ori_status(_id):
    
    status ={}
    
    #s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
    count,get_results = s_weibo.search(query={'_id': _id},fields=['text','_id','geo','source','retweeted_mid','reposts_count','comments_count','attitudes_count','user','timestamp'])
    print 'yuan',count
    for r in get_results():
        status['text'] = r['text']
        status['id'] = r['_id']
        if r['geo']:
            status['geo'] = r['geo']
        else:
            status['geo'] = None
        #print r['source']
        status['sourcePlatform'] = 'None'#re.match('<.*?>(.*)<.*?>', r['source']).group(1).decode("utf-8")
        if r['retweeted_mid']:
            status['retweetedMid'] = r['retweeted_mid']
        else:
            status['retweetedMid'] = None
        status['repostsCount'] = r['reposts_count']
        status['commentsCount'] = r['comments_count']
        status['attitudesCount'] = r['attitudes_count']
        if r['user']: 
            status['user'] = get_user(r['user'])
        else:
            status['user'] = {'name':'未知用户'}

        status['timestamp'] =r['timestamp']
        status['postDate'] = datetime.fromtimestamp(r['timestamp'])
        break
    return status
    
def calculate_single(_id):

    #初始化
    blog_info = {}
    city_count = {}
    html = '''<select name="province" id="province" defvalue="11"><option value="34">安徽</option><option value="11">北京</option><option value="50">重庆</option><option value="35">福建</option><option value="62">甘肃</option>
                <option value="44">广东</option><option value="45">广西</option><option value="52">贵州</option><option value="46">海南</option><option value="13">河北</option>
                <option value="23">黑龙江</option><option value="41">河南</option><option value="42">湖北</option><option value="43">湖南</option><option value="15">内蒙古</option><option value="32">江苏</option>
                <option value="36">江西</option><option value="22">吉林</option><option value="21">辽宁</option><option value="64">宁夏</option><option value="63">青海</option><option value="14">山西</option><option value="37">山东</option>
                <option value="31">上海</option><option value="51">四川</option><option value="12">天津</option><option value="54">西藏</option><option value="65">新疆</option><option value="53">云南</option><option value="33">浙江</option>
                <option value="61">陕西</option><option value="71">台湾</option><option value="81">香港</option><option value="82">澳门</option><option value="400">海外</option><option value="100">其他</option></select>'''
    province_soup = BeautifulSoup(html)
    for province in province_soup.findAll('option'):
        pp = province.string
        if pp == u'海外' or pp == u'其他':
            continue
        city_count[pp] = 0

        
    begin_ts1 = time.mktime(datetime(2013, 1, 1).timetuple())
    now = date.today()
    now_year = int(now.year)
    now_month = int(now.month)
    now_day = int(now.day)
    end_ts1 = time.mktime(datetime(now_year, now_month, now_day).timetuple())
    
    #获取原微博信息
    status_ori = get_ori_status(_id)

    #获取相关微博
    #s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
    count,get_results = s_weibo.search(query={'retweeted_mid': _id,'timestamp': {'$gt': begin_ts1, '$lt': end_ts1} }, sort_by=['timestamp'])

    print count
    reposter = []
    
    date_list = []
    date_list.append(date.fromtimestamp(status_ori['timestamp']))
    perday_repost_count = []
    perday_repost_count.append(1)
    
    per = date(2000,01,02)-date(2000,01,01)
    reposts_sum = 0
    comments_sum = 0
    key_reposter = []

    for r in get_results():
      
        if r['user']:
            user = get_user(r['user'])
            if user['location'] != None:
                p = user['location']
                tp = p.split(' ')
                ppp = tp[0]
                if ppp == u'海外' or ppp == u'其他':
                    pass
                else:
                    city_count[ppp] += 1
            if user not in reposter:
                reposter.append(user)
            if r['reposts_count'] > 1000:
                key_reposter.append(user)
        else:
             pass

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
    medias = db.session.query(Media)
    for key in reposter:
       if key in medias:
          media_index += 1
       else:
          pass

    #print city_count
    map_data = province_color_map(city_count)
    #print map_data
    leader_index = len(key_reposter)
    
    blog_info['status'] = status_ori
    blog_info['user'] = status_ori['user']
    blog_info['repost_users'] = reposter
    blog_info['datelist'] = date_list
    blog_info['perday_count'] = perday_repost_count
    blog_info['persistent_index'] = persistent_index
    blog_info['sudden_index'] = sudden_index
    blog_info['coverage_index'] = coverage_index
    blog_info['media_index'] = media_index
    blog_info['leader_index'] = leader_index
    blog_info['geo'] = map_data
    blog_info['key_reposter'] = key_reposter
    
    return blog_info
