# -*- coding: utf-8 -*-

from __future__ import division
import time
import  calendar
import re
from datetime import datetime
from datetime import date

from weibo.model import *
from weibo.extensions import db

from xapian_weibo.xapian_backend import XapianSearch
from BeautifulSoup import BeautifulSoup
from city_color import province_color_map
from weibo.global_config import xapian_search_user as user_search
#user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user')

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

def calculate(results):
    #初始化
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
    topic_participents_uid = set()
    
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
        

    for r in results:
        # 获取时间与每天微博数量
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
                    topic_participents_uid.add(uid)
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
    medias = db.session.query(Media)
    for media in medias:
	media_name = media.mediaName
	top_medias.append(media_name)
	
    media_list = []
    # for r in topic_ori_blog:
    #     tmedia = []
    #     tmedia.append(r['user']['name'])
    #     x = r['status']['comments_count']+r['status']['reposts_count']
    #     tmedia.append(x)
    #     media_list.append(tmedia)
    #     sorted(media_list, key=lambda tmedia: tmedia[1],reverse = True)
    #     if len(media_list) >= 20:
    #         m = 0
    #         while m < 20:
    #     	if media_list[m][0] in top_medias:
    #     	    media_index += 1
    #     	    m += 1
    #     	else:
    #     	    m += 1
    #     else:
    #         m = 0
    #         while m < len(media_list):
    #     	if media_list[m][0] in top_medias:
    #     	    media_index += 1
    #     	    m += 1
    #     	else:
    #     	    m += 1

    leader_index = len(topic_leader)

    work_list = []
    work_count = []
    fields = db.session.query(Field)
    for field in fields:
	field_name = field.fieldName
	work_list.append(field_name)
	work_count.append(0)
    for r in topic_participents:
	k = 0
	while k < len(work_list):
	    if r['userField'] == work_list[k]:
		work_count[k] += 1
		break
	    else:
		k += 1
    
    topic_index['persistent_index'] = persistent_index
    topic_index['sudden_index'] = sudden_index
    topic_index['coverage_index'] = coverage_index
    topic_index['media_index'] = media_index
    topic_index['leader_index'] = leader_index

    map_data = province_color_map(city_count)

    topic_info['topic_poster'] = topic_participents[0]['name']
    topic_info['topic_post_date'] = date_list[0]
    topic_info['topic_leader_count'] = len(topic_leader)
    topic_info['topic_participents'] = len(topic_participents)
    topic_info['blogs_sum'] = blogs_sum
    topic_info['topic_ori_blog_count'] = len(topic_ori_blog)
    topic_info['topic_url'] = topic_url
    topic_info['perday_count_list'] = perday_count_list
    topic_info['date_list'] = date_list
    topic_info['topic_rel_blog'] = topic_rel_blog
    topic_info['geo'] = map_data
    topic_info['topic_leader'] = topic_leader
    topic_info['topic_working_list'] = work_list
    topic_info['topic_working_count'] = work_count
    topic_info['topic_index'] = topic_index

    print 'calculate is done in %s seconds' % (time.time() - start_time)
            
    return topic_info
