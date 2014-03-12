# -*- coding: utf-8 -*-

from __future__ import division
import  calendar
import re
import os
import sys
import json
import heapq
import time
from datetime import date
from datetime import datetime
from model import *
from config import db
from xapian_weibo.xapian_backend import XapianSearch
from BeautifulSoup import BeautifulSoup
from city_color import province_color_map
from graph import graph
from pyelevator import WriteBatch, Elevator
from global_config import xapian_search_user as user_search
path = '/home/ubuntu12/dev/data/stub/master_timeline_weibo_'

try:
    from weibo.global_config import LEVELDBPATH
except:
    LEVELDBPATH = '/media/data/leveldb/'
    print 'not in web environment'


def ts2datetimestr(ts):
    return time.strftime('%Y%m%d', time.localtime(ts))

def date2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

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

def getNone():
    user = dict()
    user['_id'] = 'None'
    user['province'] = 'None'
   # user['bi_followers_count'] = 'None'
    user['verified'] = 'None'
    user['description'] = 'None'
    user['friends_count'] = 0
    user['city'] = 'None'
    user['gender']  = 'None'
    user['profile_image_url'] = 'None'
    user['verified_type'] = 'None'
    user['followers_count'] = 0
    user['location'] = 'None'
    user['statuses_count'] = 0
    user['name'] = 'None'
    return user

def _default_elevator(db_name='default'):
    db = Elevator(db_name, transport='tcp', endpoint='192.168.2.31:4141')
    return db


def init_db():
    E = _default_elevator()
    E.createdb(os.path.join(LEVELDBPATH, 'linhao_weibo_gexf_tree'))

    E.disconnect()


def save_weibo_tree(mid, whole_g, whole_stats, sub_g, sub_stats):
    E = _default_elevator(os.path.join(LEVELDBPATH, 'linhao_weibo_gexf_tree'))
    E.Put(str(mid), whole_g + '_\/' + json.dumps(whole_stats) + '_\/' + sub_g + '_\/' + json.dumps(sub_stats))
    E.disconnect()
#    print mid,whole_g,whole_stats,sub_g,sub_stats

    
def calculate_single(_id):
    # 转发树（全树与子树计算）
    g = graph(_id)
    whole_g = g['whole']
    sub_g = g['sub']

    whole_g_graph = whole_g['graph']
    whole_g_stats = whole_g['stats']
    whole_g_reposts = whole_g['reposts']

    sub_g_graph = sub_g['graph']
    sub_g_stats = sub_g['stats']
    sub_g_reposts = sub_g['reposts']

    save_weibo_tree(str(_id), whole_g_graph, whole_g_stats, sub_g_graph, sub_g_stats)
    calculate_single_whole(whole_g_reposts, whole_g['ori'], sub_g['ori'])
    calculate_single_sub(sub_g_reposts, sub_g['ori'])
    return 'Done'


def calculate_single_whole(whole_g_reposts, retweeted_ori, mid_ori):
    #初始化
    blog_info = {}
    city_count = {}
    topic_rel_blog = dict()
    province_name = dict()

    html = '''<select name="province" id="province" defvalue="11"><option value="34">安徽</option><option value="11">北京</option><option value="50">重庆</option><option value="35">福建</option><option value="62">甘肃</option><option value="44">广东</option>
              <option value="45">广西</option><option value="52">贵州</option><option value="46">海南</option><option value="13">河北</option><option value="23">黑龙江</option><option value="41">河南</option><option value="42">湖北</option>
              <option value="43">湖南</option><option value="15">内蒙古</option><option value="32">江苏</option><option value="36">江西</option><option value="22">吉林</option><option value="21">辽宁</option><option value="64">宁夏</option>
              <option value="63">青海</option><option value="14">山西</option><option value="37">山东</option><option value="31">上海</option><option value="51">四川</option><option value="12">天津</option><option value="54">西藏</option>
              <option value="65">新疆</option><option value="53">云南</option><option value="33">浙江</option><option value="61">陕西</option><option value="71">台湾</option><option value="81">香港</option><option value="82">澳门</option>
              <option value="400">海外</option><option value="100">其他</option></select>'''
    
    province_soup = BeautifulSoup(html)

    for province in province_soup.findAll('option'):
        pp = province.string
        key = province['value']
        province_name[key] = pp
        if pp == u'海外' or pp == u'其他':
            continue
        city_count[pp] = 0
    
    reposter = set() # 转发用户集合
    date_list = []
    perday_repost_count = []
    reposts_sum = 0
    comments_sum = 0
    key_reposter = dict()
    weibo_rank = dict()
    topic_user = dict()
    
    retweeted_timestamp = retweeted_ori['timestamp']
    retweeted_ori['postDate'] = date.fromtimestamp(retweeted_timestamp)
    try:
        retweeted_ori['repostsCount'] = int(retweeted_ori['reposts_count'])
    except:
        retweeted_ori['repostsCount'] = 0
    try:
        retweeted_ori['commentsCount'] = int(retweeted_ori['comments_count'])
    except:
        retweeted_ori['commentsCount'] = 0
    try:
        retweeted_ori['attitudesCount'] = int(retweeted_ori['attitudes_count'])
    except:
        retweeted_ori['attitudesCount'] = 0
    try:
        retweeted_ori['text'] = retweeted_ori['text']
    except:
        retweeted_ori['text'] = ''
    date_list.append(date.fromtimestamp(retweeted_timestamp))
    perday_repost_count.append(1)
    
    mid_timestamp = mid_ori['timestamp']
    mid_ori['postDate'] = date.fromtimestamp(mid_timestamp)
    try:
        mid_ori['repostsCount'] = int(mid_ori['reposts_count'])
    except:
        mid_ori['repostsCount'] = 0
    try:
        mid_ori['commentsCount'] = int(mid_ori['comments_count'])
    except:
        mid_ori['commentsCount'] = 0
    try:
        mid_ori['attitudesCount'] = int(mid_ori['attitudes_count'])
    except:
        mid_ori['attitudesCount'] = 0
    try:
        mid_ori['text'] = mid_ori['text']
    except:
        mid_ori['text'] = ''
    
    
    leader_index = 0
    iter_count = 0
    for r in whole_g_reposts:
        friends_count = 0
        followers_count = 0
        statuses_count = 0
        reposts_count = 0
        comments_count = 0
        attitudes_count = 0
        try:
            reposts_count += r['reposts_count']
        except:
            pass
        try:
            comments_count += r['comments_count']
        except:
            pass
        try:
            attitudes_count += r['attitudes_count']
        except:
            pass
        try:
            created_date = date.fromtimestamp(r['timestamp'])
        except:
            continue
        try:
            text = r['text']
        except:
            text = ''

        if 'user' in r and r['user']:
            user = r['user']
            userId = user['id']

            # 将用户加入转发者集合
            reposter.add(userId)
            
            # 统计转发用户注册省份
            if 'province' in user and user['province']:
                p = province_name[user['province']]
                if p == u'海外' or p == u'其他':
                    pass
                else:
                    city_count[p] += 1
            
            # leader_index
            if r['reposts_count'] > 1000:
                leader_index = leader_index + 1
            
            # 生成用户对应的排序指标
            try:
                friends_count += user['friends_count']
            except:
                pass
            try:
                followers_count += user['followers_count']
            except:
                pass
            try:
                statuses_count += user['statuses_count']
            except:
                pass
            if userId not in topic_user.keys():
                topic_user[userId] = friends_count + followers_count + statuses_count
            
            # 存储用户信息
            if userId not in key_reposter.keys():
                key_reposter[userId] = user

            # 存储微博信息
            temp = {}
            temp['status'] = r
            temp['status']['repostsCount'] = reposts_count
            temp['status']['commentsCount'] = comments_count
            temp['status']['attitudesCount'] = attitudes_count
            temp['status']['created_at'] = created_date
            temp['status']['postDate'] = created_date
            temp['status']['text'] = text
            temp['status']['source'] = 'unknown'
            temp['user'] = user
            topic_rel_blog[r['_id']] = temp
            
            # 生成微博对应的排序指标
            weibo_rank[r['_id']] = reposts_count
            
        else:
             continue

        tempdate = created_date
        if tempdate < date_list[-1]:
            if tempdate in date_list:
                temp_index = date_list.index(tempdate)
                perday_repost_count[temp_index] += 1
            else:
                i = 0
                while i < len(date_list):
                    if tempdate > date_list[i] and tempdate < date_list[i+1]:
                        date_list.insert(i+1, tempdate)
                        perday_repost_count.insert(i+1, 1)
                        break
                    else:
                        i += 1

        elif tempdate == date_list[-1]:
            perday_repost_count[-1] += 1

        else:
            timedelta = date(2000, 1, 2)-date(2000, 1, 1)
            while date_list[-1] != tempdate:
                tempdate1 = date_list[-1] + timedelta
                date_list.append(tempdate1)
                perday_repost_count.append(0)
            perday_repost_count[-1] = 1

        reposts_sum += reposts_count
        comments_sum += comments_count

        iter_count = iter_count + 1
        if iter_count % 10000 == 0:
            print iter_count
    
    # totalRepost & avg
    totalRepost = reposts_sum + 1
    if len(date_list):
        avg = (float(totalRepost)) / len(date_list)
    else:
        avg = 0
    
    # persistent
    persistent_index = 0
    sudden_count = 0
    j = 0
    while j < len(date_list):
       if perday_repost_count[j] > avg:
          persistent_index += 1
          sudden_count = perday_repost_count[j] - avg + sudden_count
          j += 1
       else:
          j += 1
    
    #  sudden
    if totalRepost:
        sudden_index = '%10.2f' % ( (float(sudden_count)) / totalRepost )
    else:
        sudden_index = 0
    
    # coverage
    if len(date_list):
        coverage_index = '%10.2f'%((totalRepost+comments_sum)/float(24*len(date_list)))
    else:
        coverage_index = 0
    
    # media
    media_index = 0
    medias = db.session.query(IMedia).all()
    for media in medias:
        key = media.mediaID
        if key in reposter:
            media_index += 1
    
    # 用户排序
    user_th = TopkHeap(1000)
    for d, x in topic_user.items():
        user_th.Push((x, d)) # 排序指标、id

    user_data = user_th.TopK()
    
    # 微博排序
    weibo_th = TopkHeap(1000)
    for d,x in weibo_rank.items():
        weibo_th.Push((x,d)) # 排序指标、id

    weibo_data = weibo_th.TopK()
    
    blog_info['status'] = mid_ori
    blog_info['user'] = mid_ori['user']
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
    
    if not blog_info['user']:
        blog_info['user'] = getNone()
    save_base_infor(blog_info['status']['_id'], blog_info['user']['profile_image_url'],blog_info['status']['text'],blog_info['status']['source'],blog_info['status']['postDate'],blog_info['user']['id'],blog_info['user']['name'],blog_info['status']['repostsCount'],blog_info['status']['commentsCount'],blog_info['status']['attitudesCount'],blog_info['persistent_index'],blog_info['sudden_index'],blog_info['coverage_index'],blog_info['media_index'],blog_info['leader_index'])    

    perday_blog_count = blog_info['perday_count']
    date_list = blog_info['datelist']   
    for i in range(0,len(date_list)):
        save_daily_count(blog_info['status']['_id'],date_list[i],perday_blog_count[i])

    exist_items = db.session.query(PropagateSpatialSingle).filter(PropagateSpatialSingle.mid==blog_info['status']['_id']).all()
    for exist_item in exist_items:
        db.session.delete(exist_item)
    db.session.commit()
    
    for d,x in blog_info['geo'].items():
        save_map(blog_info['status']['_id'],d,x)

    for i in range(0,len(blog_info['key_reposter'])):
        uid = blog_info['key_reposter'][i][1]
        save_user(blog_info['status']['_id'],uid,key_reposter[uid]['name'],key_reposter[uid]['location'],key_reposter[uid]['followers_count'],key_reposter[uid]['friends_count'],key_reposter[uid]['statuses_count'],key_reposter[uid]['description'],key_reposter[uid]['profile_image_url'])

    if len(blog_info['weibo'])>=5:
        top_weibo = blog_info['weibo'][:5]
    else:
        top_weibo = blog_info['weibo']

    exist_items = db.session.query(PropagateWeiboSingle).\
                             filter(PropagateWeiboSingle.ori_mid==blog_info['status']['_id']).all()
    for exist_item in exist_items:
        db.session.delete(exist_item)
    db.session.commit()

    for i in range(0,len(top_weibo)):
        mid = top_weibo[i][1]
        save_weibo(blog_info['status']['_id'],mid,topic_rel_blog[mid]['user']['profile_image_url'],topic_rel_blog[mid]['status']['text'],topic_rel_blog[mid]['status']['source'],topic_rel_blog[mid]['status']['created_at'],topic_rel_blog[mid]['user']['id'],topic_rel_blog[mid]['user']['name'],topic_rel_blog[mid]['status']['repostsCount'],topic_rel_blog[mid]['status']['commentsCount'],topic_rel_blog[mid]['status']['attitudesCount'])

    return 'Done'

def calculate_single_sub(sub_g_reposts, retweeted_ori):
    #初始化
    blog_info = {}
    city_count = {}
    topic_rel_blog = dict()
    province_name = dict()

    html = '''<select name="province" id="province" defvalue="11"><option value="34">安徽</option><option value="11">北京</option><option value="50">重庆</option><option value="35">福建</option><option value="62">甘肃</option><option value="44">广东</option>
              <option value="45">广西</option><option value="52">贵州</option><option value="46">海南</option><option value="13">河北</option><option value="23">黑龙江</option><option value="41">河南</option><option value="42">湖北</option>
              <option value="43">湖南</option><option value="15">内蒙古</option><option value="32">江苏</option><option value="36">江西</option><option value="22">吉林</option><option value="21">辽宁</option><option value="64">宁夏</option>
              <option value="63">青海</option><option value="14">山西</option><option value="37">山东</option><option value="31">上海</option><option value="51">四川</option><option value="12">天津</option><option value="54">西藏</option>
              <option value="65">新疆</option><option value="53">云南</option><option value="33">浙江</option><option value="61">陕西</option><option value="71">台湾</option><option value="81">香港</option><option value="82">澳门</option>
              <option value="400">海外</option><option value="100">其他</option></select>'''
    
    province_soup = BeautifulSoup(html)

    for province in province_soup.findAll('option'):
        pp = province.string
        key = province['value']
        province_name[key] = pp
        if pp == u'海外' or pp == u'其他':
            continue
        city_count[pp] = 0
    
    reposter = set() # 转发用户集合
    date_list = []
    perday_repost_count = []
    reposts_sum = 0
    comments_sum = 0
    key_reposter = dict()
    weibo_rank = dict()
    topic_user = dict()
    
    retweeted_timestamp = retweeted_ori['timestamp']
    retweeted_ori['postDate'] = date.fromtimestamp(retweeted_timestamp)
    try:
        retweeted_ori['repostsCount'] = int(retweeted_ori['reposts_count'])
    except:
        retweeted_ori['repostsCount'] = 0
    try:
        retweeted_ori['commentsCount'] = int(retweeted_ori['comments_count'])
    except:
        retweeted_ori['commentsCount'] = 0
    try:
        retweeted_ori['attitudesCount'] = int(retweeted_ori['attitudes_count'])
    except:
        retweeted_ori['attitudesCount'] = 0
    try:
        retweeted_ori['text'] = retweeted_ori['text']
    except:
        retweeted_ori['text'] = ''

    date_list.append(date.fromtimestamp(retweeted_timestamp))
    perday_repost_count.append(1)
    
    leader_index = 0
    iter_count = 0
    for r in sub_g_reposts:
        friends_count = 0
        followers_count = 0
        statuses_count = 0
        reposts_count = 0
        comments_count = 0
        attitudes_count = 0
        try:
            reposts_count += r['reposts_count']
        except:
            pass
        try:
            comments_count += r['comments_count']
        except:
            pass
        try:
            attitudes_count += r['attitudes_count']
        except:
            pass
        try:
            created_date = date.fromtimestamp(r['timestamp'])
        except:
            continue
        try:
            text = r['text']
        except:
            text = ''

        if 'user' in r and r['user']:
            user = r['user']
            userId = user['id']

            # 将用户加入转发者集合
            reposter.add(userId)
            
            # 统计转发用户注册省份
            if 'province' in user and user['province']:
                p = province_name[user['province']]
                if p == u'海外' or p == u'其他':
                    pass
                else:
                    city_count[p] += 1
            
            # leader_index
            if r['reposts_count'] > 1000:
                leader_index = leader_index + 1
            
            # 生成用户对应的排序指标
            try:
                friends_count += user['friends_count']
            except:
                pass
            try:
                followers_count += user['followers_count']
            except:
                pass
            try:
                statuses_count += user['statuses_count']
            except:
                pass
            if userId not in topic_user.keys():
                topic_user[userId] = friends_count + followers_count + statuses_count
            
            # 存储用户信息
            if userId not in key_reposter.keys():
                key_reposter[userId] = user

            # 存储微博信息
            temp = {}
            temp['status'] = r
            temp['status']['repostsCount'] = reposts_count
            temp['status']['commentsCount'] = comments_count
            temp['status']['attitudesCount'] = attitudes_count
            temp['status']['created_at'] = created_date
            temp['status']['postDate'] = created_date
            temp['status']['text'] = text
            temp['status']['source'] = 'unknown'
            temp['user'] = user
            topic_rel_blog[r['_id']] = temp
            
            # 生成微博对应的排序指标
            weibo_rank[r['_id']] = reposts_count
            
        else:
             continue

        tempdate = created_date
        if tempdate < date_list[-1]:
            if tempdate in date_list:
                temp_index = date_list.index(tempdate)
                perday_repost_count[temp_index] += 1
            else:
                i = 0
                while i < len(date_list):
                    if tempdate > date_list[i] and tempdate < date_list[i+1]:
                        date_list.insert(i+1, tempdate)
                        perday_repost_count.insert(i+1, 1)
                        break
                    else:
                        i += 1

        elif tempdate == date_list[-1]:
            perday_repost_count[-1] += 1

        else:
            timedelta = date(2000, 1, 2)-date(2000, 1, 1)
            while date_list[-1] != tempdate:
                tempdate1 = date_list[-1] + timedelta
                date_list.append(tempdate1)
                perday_repost_count.append(0)
            perday_repost_count[-1] = 1

        reposts_sum += reposts_count
        comments_sum += comments_count

        iter_count = iter_count + 1
        if iter_count % 10000 == 0:
            print iter_count
    
    # totalRepost & avg
    totalRepost = reposts_sum + 1
    if len(date_list):
        avg = (float(totalRepost)) / len(date_list)
    else:
        avg = 0
    
    # persistent
    persistent_index = 0
    sudden_count = 0
    j = 0
    while j < len(date_list):
       if perday_repost_count[j] > avg:
          persistent_index += 1
          sudden_count = perday_repost_count[j] - avg + sudden_count
          j += 1
       else:
          j += 1
    
    #  sudden
    if totalRepost:
        sudden_index = '%10.2f' % ( (float(sudden_count)) / totalRepost )
    else:
        sudden_index = 0
    
    # coverage
    if len(date_list):
        coverage_index = '%10.2f'%((totalRepost+comments_sum)/float(24*len(date_list)))
    else:
        coverage_index = 0
    
    # media
    media_index = 0
    medias = db.session.query(IMedia).all()
    for media in medias:
        key = media.mediaID
        if key in reposter:
            media_index += 1
    
    # 用户排序
    user_th = TopkHeap(1000)
    for d, x in topic_user.items():
        user_th.Push((x, d)) # 排序指标、id

    user_data = user_th.TopK()
    
    # 微博排序
    weibo_th = TopkHeap(1000)
    for d,x in weibo_rank.items():
        weibo_th.Push((x,d)) # 排序指标、id

    weibo_data = weibo_th.TopK()
    
    blog_info['status'] = retweeted_ori
    blog_info['user'] = retweeted_ori['user']
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

    if not blog_info['user']:
        blog_info['user'] = getNone()
    save_base_infor_part(blog_info['status']['_id'],blog_info['user']['profile_image_url'],blog_info['status']['text'],blog_info['status']['source'],blog_info['status']['postDate'],blog_info['user']['id'],blog_info['user']['name'],blog_info['status']['repostsCount'],blog_info['status']['commentsCount'],blog_info['status']['attitudesCount'],blog_info['persistent_index'],blog_info['sudden_index'],blog_info['coverage_index'],blog_info['media_index'],blog_info['leader_index'])    

    perday_blog_count = blog_info['perday_count']
    date_list = blog_info['datelist']   
    for i in range(0,len(date_list)):
        save_daily_count_part(blog_info['status']['_id'],date_list[i],perday_blog_count[i])

    exist_items = db.session.query(PropagateSpatialSinglePart).\
                            filter(PropagateSpatialSinglePart.mid==blog_info['status']['_id']).all()
    for exist_item in exist_items:
        db.session.delete(exist_item)
    db.session.commit()
    
    for d,x in blog_info['geo'].items():
        save_map_part(blog_info['status']['_id'],d,x)

    for i in range(0,len(blog_info['key_reposter'])):
        uid = blog_info['key_reposter'][i][1]
        save_user_part(blog_info['status']['_id'],uid,key_reposter[uid]['name'],key_reposter[uid]['location'],key_reposter[uid]['followers_count'],key_reposter[uid]['friends_count'],key_reposter[uid]['statuses_count'],key_reposter[uid]['description'],key_reposter[uid]['profile_image_url'])

    if len(blog_info['weibo'])>=5:
        top_weibo = blog_info['weibo'][:5]
    else:
        top_weibo = blog_info['weibo']

    exist_items = db.session.query(PropagateWeiboSinglePart).\
                             filter(PropagateWeiboSinglePart.ori_mid==blog_info['status']['_id']).all()
    for exist_item in exist_items:
        db.session.delete(exist_item)
    db.session.commit()

    for i in range(0,len(top_weibo)):
        mid = top_weibo[i][1]
        save_weibo_part(blog_info['status']['_id'],mid,topic_rel_blog[mid]['user']['profile_image_url'],topic_rel_blog[mid]['status']['text'],topic_rel_blog[mid]['status']['source'],topic_rel_blog[mid]['status']['postDate'],topic_rel_blog[mid]['user']['id'],topic_rel_blog[mid]['user']['name'],topic_rel_blog[mid]['status']['repostsCount'],topic_rel_blog[mid]['status']['commentsCount'],topic_rel_blog[mid]['status']['attitudesCount'])

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
    
    exist_items = db.session.query(PropagateSinglePart).filter(PropagateSinglePart.mid==mid).all()
    for exist_item in exist_items:
        db.session.delete(exist_item)
    db.session.commit()

#    print mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount, persistent, sudden, coverage, media, leader
    new_item = PropagateSinglePart(mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount, persistent, sudden, coverage, media, leader)
    db.session.add(new_item)
    db.session.commit()

def save_daily_count_part(mid,date,perday_blog_count):#mid、日期、微博数

    date = ts2datetime(time.mktime(date.timetuple()))
    perday_blog_count = int(perday_blog_count)

    exist_items = db.session.query(PropagateTrendSinglePart).\
                           filter(PropagateTrendSinglePart.id==mid, \
                                  PropagateTrendSinglePart.date==date).all()
    for exist_item in exist_items:
        db.session.delete(exist_item)
    db.session.commit()

#    print mid,date,perday_blog_count
    new_item = PropagateTrendSinglePart(mid,date,perday_blog_count)
    db.session.add(new_item)
    db.session.commit()

def save_map_part(mid,city,count):#mid、城市、数量

    count = int(count)

#    print mid,city,count
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

    exist_items = db.session.query(PropagateUserSinglePart).\
                             filter(PropagateUserSinglePart.mid==mid, \
                                    PropagateUserSinglePart.user==user).all()
    for exist_item in exist_items:
        db.session.delete(exist_item)
    db.session.commit()

#    print mid,user,name,location,follower,friend,status,description,profile_image_url
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

#    print ori_id,mid,image_url,text,sourcePlatform,postDate,uid,user_name,repostsCount,commentsCount,attitudesCount
    new_item = PropagateWeiboSinglePart(ori_id,mid,image_url,text,sourcePlatform,postDate,uid,user_name,repostsCount,commentsCount,attitudesCount)
    db.session.add(new_item)
    db.session.commit()

def save_base_infor(mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount, persistent, sudden, coverage, media, leader):
    #微博id、头像url、内容、来源、发布时间、用户id、用户昵称、转发数、评论数、赞数、4个指标
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

    exist_items = db.session.query(PropagateSingle).filter(PropagateSingle.mid==mid).all()
    for exist_item in exist_items:
        db.session.delete(exist_item)
    db.session.commit()

#    print mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount, persistent, sudden, coverage, media, leader
    new_item = PropagateSingle(mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount, persistent, sudden, coverage, media, leader)
    db.session.add(new_item)
    db.session.commit()

def save_daily_count(mid,date,perday_blog_count):#mid、日期、微博数

    date = ts2datetime(time.mktime(date.timetuple()))
    perday_blog_count = int(perday_blog_count)

    exist_items = db.session.query(PropagateTrendSingle).\
                       filter(PropagateTrendSingle.id==mid, \
                              PropagateTrendSingle.date==date).all()
    for exist_item in exist_items:
        db.session.delete(exist_item)
    db.session.commit()

#    print mid,date,perday_blog_count
    new_item = PropagateTrendSingle(mid,date,perday_blog_count)
    db.session.add(new_item)
    db.session.commit()

def save_map(mid,city,count):#mid、城市、数量
    count = int(count)

#    print mid,city,count
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

    exist_items = db.session.query(PropagateUserSingle).\
                                   filter(PropagateUserSingle.mid==mid, \
                                          PropagateUserSingle.user==user).all()
    for exist_item in exist_items:
        db.session.delete(exist_item)
    db.session.commit()

#    print mid,user,name,location,follower,friend,status,description,profile_image_url
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

#    print ori_id,mid,image_url,text,sourcePlatform,postDate,uid,user_name,repostsCount,commentsCount,attitudesCount
    new_item = PropagateWeiboSingle(ori_id,mid,image_url,text,sourcePlatform,postDate,uid,user_name,repostsCount,commentsCount,attitudesCount)
    db.session.add(new_item)
    db.session.commit()

if __name__ == "__main__":
##    init_db()

    # topic_info = calculate_part(3617839380294898, 1377964800, 1378224000,[3617782506173763,3618043278635735,3618455003121922,3618481590955662,3618479728507301])
    # print topic_info

    # topic_info = calculate_single(3617726042418839)
    # print topic_info

    # topic_info = calculate_single(3618201981966170)
    # print topic_info

    topic_info = calculate_single(3618204893345603)
    print topic_info
