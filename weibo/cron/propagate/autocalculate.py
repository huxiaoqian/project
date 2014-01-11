# -*- coding: utf-8 -*-

from __future__ import division
import time
import  calendar
import re
import os
from datetime import datetime
from datetime import date

from model import *#PropagateTopic, PropagateTrend, PropagateSpatial, PropagateUser, PropagateWeibo
from config import db

from xapian_weibo.xapian_backend import XapianSearch
from BeautifulSoup import BeautifulSoup
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

def getXapianweiboByTs(start_time, end_time):
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

def get_user(uid):
    user = {}
    count,get_results = user_search.search(query={'_id': uid})
    for r in get_results():
        user['id'] = r['_id']
        user['province'] = r['province']
        user['bi_followers_count'] = 'None'
        user['verified'] = r['verified']
        user['description'] = r['description']
        user['friends_count'] = r['friends_count']
        user['city'] = r['city']
        user['gender']  = r['gender']
        user['profile_image_url'] = r['profile_image_url']
        user['verified_reason'] = 'None'
        user['followers_count'] = r['followers_count']
        user['location'] = r['location']
        #user['active'] = r['active']
        user['statuses_count'] = r['statuses_count']
        if r['name']:
            user['name'] = r['name']
        else:
            user['name'] = u'未知用户'
        user['userField'] = u'未知领域'
        break
    if user == {}:
        return None
    else:
        return user

def calculate(keyword, beg_time, end_time):
    #初始化
##    beg_ts = date2ts(beg_time)
##    end_ts = date2ts(end_time)
##    keyword = keyword.decode('utf-8')
    start_time = time.time()
    topic_info = {}

    topic_index = {}
    date_list = []
    perday_count_list = []
    topic_rel_blog = []
    topic_url = []
    topic_participents = []
    topic_leader = []
    topic_date = []
    blogs_sum = 0
    comments_sum = 0
    topic_ori_blog = []

    topic_leader_uid = set()
    topic_participents_uid = []
    
    city_count={}
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

    statuses_search = getXapianweiboByTs(beg_time, end_time)
    fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
    count, get_results = statuses_search.search(query={'text': [u'%s'%keyword]}, sort_by=['reposts_count'], fields=fields_list,max_offset=1000)
    for r in get_results():
        # 获取时间与每天微博数量
        if not r['reposts_count']:
            r['reposts_count'] = 0
        if not r['comments_count']:
            r['comments_count'] = 0
        if not r['attitudes_count']:
            r['attitudes_count'] = 0
	temp_date = date.fromtimestamp(r['timestamp'])
        if len(date_list) == 0:
            date_list.append(temp_date)
            perday_count_list.append(1)
        else:
            if temp_date < date_list[-1]:
                if temp_date in date_list:
                    temp_index = date_list.index(temp_date)
                    perday_count_list[temp_index] += 1
                else:
                    i = 0
                    while i < len(date_list):
                        if temp_date < date_list[0]:
                            date_list.insert(0,temp_date)
                            perday_count_list.insert(0,1)
                            break
                        else:
                            if temp_date > date_list[i] and temp_date < date_list[i+1]:
                                date_list.insert(i+1,temp_date)
                                perday_count_list.insert(i+1,1)
                                break
                            else:
                                i += 1
            if temp_date == date_list[-1]:
                perday_count_list[-1] += 1
            if temp_date > date_list[-1]:
                timedelta = date(2000,1,2)-date(2000,1,1)
                while date_list[-1] != temp_date:
                    temp_date1 = date_list[-1] + timedelta
                    date_list.append(temp_date1)
                    perday_count_list.append(0)
                perday_count_list[-1] = 1
                        

	if r['user']:
            uid = int(r['user'])
            user = get_user(uid)
            if user != None:
                if uid not in topic_participents_uid:
                    topic_participents_uid.append(uid)
                    topic_participents.append(user)
                if r['retweeted_mid'] == None:
                    temp_ori = {}
                    temp_ori['status'] = r
                    temp_ori['user'] = user
                    topic_ori_blog.append(temp_ori)
                if r['reposts_count'] != None and r['comments_count'] != None:
                    rc = r['reposts_count'] + r['comments_count']
                    if rc > 1500:
                        if uid not in topic_leader_uid:
                            topic_leader_uid.add(uid)
                            topic_leader.append(user)                  
                if r['reposts_count'] > 1000:
                    temp = {}
                    temp['status'] = r
                    temp['status']['created_at'] = datetime.fromtimestamp(r['timestamp'])
                    temp['status']['text'] = r['text']
                    temp['status']['source'] = 'None'
                    temp['user'] = user
                    topic_rel_blog.append(temp)
                if r['bmiddle_pic']:
                    topic_url.append(r['bmiddle_pic'])
                if user['province'] != None:
                    p = province_name[user['province']]
                    if p == u'海外' or p == u'其他':
                        pass
                    else:
                        city_count[p] += 1
                else:
                    pass
        else:
            pass

        if r['comments_count'] != None:
            comments_sum = comments_sum + r['comments_count']
        else:
            comments_sum = comments_sum + 0
        blogs_sum += 1

    print 'loop is done in %s seconds' % (time.time() - start_time)

    timedelta = len(date_list)
    avg = blogs_sum/float(timedelta)
    i = 0
    persistent_index = 0
    temp_sudden = 0
    while i < int(timedelta):
	if perday_count_list[i] > avg:
	    persistent_index += 1
	    temp_sudden = perday_count_list[i]-avg+temp_sudden
	    i += 1
	else:
	    i += 1
    sudden_index = '%10.2f'%(temp_sudden/float(blogs_sum))
    coverage_index = '%10.2f'%((blogs_sum + comments_sum)/(24*float(timedelta)))
    
    media_index = 0
    top_medias = []
    medias = db.session.query(IMedia).all()
    for media in medias:
	media_name = media.mediaName
	top_medias.append(media_name)
	
    media_list = []
    for r in topic_ori_blog:
        tmedia = []
        tmedia.append(r['user']['name'])
        x = r['status']['comments_count']+r['status']['reposts_count']
        tmedia.append(x)
        media_list.append(tmedia)
        sorted(media_list, key=lambda tmedia: tmedia[1],reverse = True)
        if len(media_list) >= 20:
            m = 0
            while m < 20:
        	if media_list[m][0] in top_medias:
        	    media_index += 1
        	    m += 1
        	else:
        	    m += 1
        else:
            m = 0
            while m < len(media_list):
        	if media_list[m][0] in top_medias:
        	    media_index += 1
        	    m += 1
        	else:
        	    m += 1

    leader_index = len(topic_leader)
    
    topic_index['persistent_index'] = persistent_index
    topic_index['sudden_index'] = sudden_index
    topic_index['coverage_index'] = coverage_index
    topic_index['media_index'] = media_index
    topic_index['leader_index'] = leader_index

    #map_data = province_color_map(city_count)

    topic_info['topic_poster'] = topic_participents[0]['name']
    topic_info['topic_post_date'] = date_list[0]
    topic_info['topic_leader_count'] = len(topic_leader)
    topic_info['topic_participents'] = topic_participents_uid
    topic_info['blogs_sum'] = blogs_sum
    topic_info['topic_ori_blog_count'] = len(topic_ori_blog)
    topic_info['topic_url'] = topic_url
    topic_info['perday_count_list'] = perday_count_list
    topic_info['date_list'] = date_list
    topic_info['topic_rel_blog'] = topic_rel_blog
    topic_info['geo'] = city_count
    topic_info['topic_index'] = topic_index

    print 'calculate is done in %s seconds' % (time.time() - start_time)

    wordid = save_base_infor(keyword,topic_info['topic_poster'],topic_info['topic_url'],topic_info['blogs_sum'],topic_info['topic_ori_blog_count'],topic_info['topic_index'],topic_info['topic_post_date'],beg_time, end_time)       

    perday_blog_count = topic_info['perday_count_list']
    date_list = topic_info['date_list']    
    for i in range(0,len(date_list)):
        save_daily_count(wordid,date_list[i],perday_blog_count[i])

    for d,x in topic_info['geo'].items():
        save_map(wordid,d,x)

    for i in range(0,len(topic_info['topic_participents'])):
        save_user(wordid,topic_info['topic_participents'][i])

    weibo = topic_info['topic_rel_blog'][:5]
    for i in range(0,len(weibo)):
        save_weibo(wordid,weibo[i])
  
    return 'Done'

def save_base_infor(keyword,topic_poster,topic_url,blogs_sum,topic_ori_blog_count,topic_index,topic_post_date,beg_ts,end_ts):#话题、发起人、头像url、微博数、原创微博数、4个指标、发起时间、起始时间、终止时间

    beg_date = ts2datetime(beg_ts)
    end_date = ts2datetime(end_ts)
    blogs_sum = int(blogs_sum)
    topic_ori_blog_count = int(topic_ori_blog_count)
    persistent = float(topic_index['persistent_index'])
    sudden = float(topic_index['sudden_index'])
    coverage = float(topic_index['coverage_index'])
    media = float(topic_index['media_index'])
    leader = float(topic_index['leader_index'])
    topic_post_date = ts2datetime(time.mktime(topic_post_date.timetuple()))

    if not topic_url:
        topic_url = 'None'
    #print keyword,topic_url,beg_date,end_date,topic_poster,blogs_sum,topic_ori_blog_count,topic_post_date,persistent,sudden,coverage,media,leader
    new_item = PropagateTopic(keyword,topic_url,beg_date,end_date,topic_poster,blogs_sum,topic_ori_blog_count,topic_post_date,persistent,sudden,coverage,media,leader)
    db.session.add(new_item)
    db.session.commit()
    db.session.refresh(new_item)
    return new_item.id

def save_daily_count(wordid,date,perday_blog_count):#话题id、日期、微博数

    date = ts2datetime(time.mktime(date.timetuple()))
    perday_blog_count = int(perday_blog_count)

    #print date,perday_blog_count
    new_item = PropagateTrend(wordid,date,perday_blog_count)
    db.session.add(new_item)
    db.session.commit()

def save_map(wordid,city,count):#话题id、城市、数量

    count = int(count)
    #city = city.decode('utf-8')
    #print city,count
    new_item = PropagateSpatial(wordid,city,count)
    db.session.add(new_item)
    db.session.commit()

def save_user(wordid,uid):#话题id、用户id

    user = str(uid)    
    #print user
    new_item = PropagateUser(wordid,user)
    db.session.add(new_item)
    db.session.commit()

def save_weibo(wordid,weibo):#话题id、微博

    mid = str(weibo['status']['_id'])
    image_url = str(weibo['user']['profile_image_url'])
    text = weibo['status']['text']
    sourcePlatform = weibo['status']['source']
    postDate = weibo['status']['created_at']
    uid = str(weibo['user']['id'])
    user_name = weibo['user']['name']
    repostsCount = int(weibo['status']['reposts_count'])
    commentsCount = int(weibo['status']['comments_count'])
    attitudesCount = int(weibo['status']['attitudes_count'])
    
    new_item = PropagateWeibo(wordid,mid,image_url,text,sourcePlatform,postDate,uid,user_name,repostsCount,commentsCount,attitudesCount)
    db.session.add(new_item)
    db.session.commit()

if __name__ == "__main__":

    topic_info = calculate('日本', '2013-09-01', '2013-09-05')
    print topic_info
##    print 'weibo:'
##    weibo = topic_info['topic_rel_blog'][:5]
##    for i in range(0,len(weibo)):
##        print weibo[i]
    #print 'map',topic_info['geo']
    #print 'index:',topic_info['topic_index']
##    print 'uid:'
##    for i in range(0,len(topic_info['topic_participents'])):
##        print topic_info['topic_participents'][i]
