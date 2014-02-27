# -*- coding: utf-8 -*-

import os
import sys
import re
import calendar
import time
from datetime import date
from datetime import datetime

from weibo.model import *
from weibo.extensions import db
from city_color import province_color_map
from BeautifulSoup import BeautifulSoup

def readPropagateTopic(keyword,beg_date,end_date):

    topic_info = dict()
    propagateTopics = db.session.query(PropagateTopic).filter((PropagateTopic.topic_name==keyword)&(PropagateTopic.start==beg_date)&(PropagateTopic.end==end_date)).all()
    
    if len(propagateTopics):
        for p in propagateTopics:
            topic_info['id'] = p.id
            topic_info['topic_poster'] = p.raise_user
            topic_info['topic_url'] = p.image_url
            topic_info['blogs_sum'] = p.t_weibo
            topic_info['topic_ori_blog_count'] = p.o_weibo
            topic_info['leader_index'] = int(p.leader)
            topic_info['topic_post_date'] = p.raise_time
        return topic_info
    else:
        return topic_info

def readPropagateTrend(keyid):

    topic_info = dict()
    propagateTrends = db.session.query(PropagateTrend).filter(PropagateTrend.topic_id==keyid).all()
    
    if len(propagateTrends):
        perday_count_list = []
        date_list = []
        for p in propagateTrends:
            perday_count_list.append(p.count)
            date_list.append(p.date)

        topic_info['perday_count_list'] = perday_count_list
        topic_info['date_list'] = date_list
        return topic_info
    else:
        return topic_info

def readPropagateWeibo(keyid):

    topic_info = []
    propagateWeibos = db.session.query(PropagateWeibo).filter(PropagateWeibo.topic_id==keyid).all()
    
    if len(propagateWeibos):
        
        for p in propagateWeibos:
            user = dict()
            status = dict()
            status['_id'] = p.mid
            user['profile_image_url'] = p.image_url
            status['text'] = p.text
            status['source'] = p.sourcePlatform
            status['created_at'] = p.postDate
            user['id'] = p.uid
            user['name'] = p.user_name
            status['reposts_count'] = p.repostsCount
            status['comments_count'] = p.commentsCount
            status['attitudes_count'] = p.attitudesCount
            topic_info.append({'status':status,'user':user})

        return topic_info
    else:
        return topic_info

def readPropagateSpatial(keyid):

    propagateSpatials = db.session.query(PropagateSpatial).filter(PropagateSpatial.topic_id==keyid).all()
    
    if len(propagateSpatials):
        city_count = dict()
        for p in propagateSpatials:
            city_count[p.city] = p.count
        map_data = province_color_map(city_count)
        return map_data
    else:
        city_count = {}
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
        map_data = province_color_map(city_count)
        return map_data

def readIndex(keyid):

    topic_info = dict()
    propagateTopics = db.session.query(PropagateTopic).filter(PropagateTopic.id==keyid).all()
    
    if len(propagateTopics):        
        for p in propagateTopics:
            topic_info['persistent_index'] = p.persistent
            topic_info['sudden_index'] = p.sudden
            topic_info['coverage_index'] = p.coverage
            topic_info['media_index'] = p.media
            topic_info['leader_index'] = p.leader       
        return topic_info
    else:
        return topic_info

def readPropagateUser(keyid):

    topic_info = []
    propagateUsers = db.session.query(PropagateUser).filter(PropagateUser.topic_id==keyid).all()
    
    if len(propagateUsers):
        for p in propagateUsers:
            user = dict()
            if p.user == 'None':
                user['id'] = 0
            else:
                user['id'] = int(p.user)
            user['name'] = p.user_name
            user['location'] = p.location
            user['follower'] = p.follower
            user['friend'] = p.friend
            user['status'] = p.status
            user['description'] = p.description
            user['image_url'] = p.image_url
            topic_info.append(user)       
        return topic_info
    else:
        return topic_info

def readPropagateSingle(mid):

    blog_info = []
    propagateSingles = db.session.query(PropagateSingle).filter(PropagateSingle.mid==mid).all()
    
    if len(propagateSingles):
        for p in propagateSingles:
            user = dict()
            if p.uid == 'None':
                user['id'] = 0
            else:
                user['id'] = int(p.uid)
            user['name'] = p.user_name
            user['repostsCount'] = p.repostsCount
            user['commentsCount'] = p.commentsCount
            user['attitudesCount'] = p.attitudesCount
            user['postDate'] = p.postDate
            user['text'] = p.text
            user['profile_image_url'] = p.image_url
            blog_info.append(user)       
        return blog_info
    else:
        return blog_info

def readPropagateTrendSingle(mid):

    blog_info = dict()
    propagateTrendSingles = db.session.query(PropagateTrendSingle).filter(PropagateTrendSingle.mid==mid).all()
    
    if len(propagateTrendSingles):
        perday_count_list = []
        date_list = []
        for p in propagateTrendSingles:
            perday_count_list.append(p.count)
            date_list.append(p.date)

        blog_info['perday_count'] = perday_count_list
        blog_info['date_list'] = date_list       
        return blog_info
    else:
        return blog_info

def readPropagateWeiboSingle(mid):

    blog_info = []
    propagateWeibos = db.session.query(PropagateWeiboSingle).filter(PropagateWeiboSingle.ori_mid==mid).all()
    
    if len(propagateWeibos):
        
        for p in propagateWeibos:
            user = dict()
            status = dict()
            status['_id'] = p.mid
            user['profile_image_url'] = p.image_url
            status['text'] = p.text
            status['source'] = p.sourcePlatform
            status['created_at'] = p.postDate
            user['id'] = p.uid
            user['name'] = p.user_name
            status['reposts_count'] = p.repostsCount
            status['comments_count'] = p.commentsCount
            status['attitudes_count'] = p.attitudesCount
            blog_info.append({'status':status,'user':user})

        return blog_info
    else:
        return blog_info

def readPropagateSpatialSingle(mid):

    propagateSpatials = db.session.query(PropagateSpatialSingle).filter(PropagateSpatialSingle.mid==mid).all()
    
    if len(propagateSpatials):
        city_count = dict()
        for p in propagateSpatials:
            city_count[p.city] = p.count
        map_data = province_color_map(city_count)
        return map_data
    else:
        city_count = {}
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
        map_data = province_color_map(city_count)
        return map_data

def readIndexSingle(mid):

    blog_info = dict()
    propagateTopics = db.session.query(PropagateSingle).filter(PropagateSingle.mid==mid).all()
    
    if len(propagateTopics):        
        for p in propagateTopics:
            blog_info['persistent_index'] = p.persistent
            blog_info['sudden_index'] = p.sudden
            blog_info['coverage_index'] = p.coverage
            blog_info['media_index'] = p.media
            blog_info['leader_index'] = p.leader       
        return blog_info
    else:
        return blog_info

def readPropagateUserSingle(mid):

    blog_info = []
    propagateUsers = db.session.query(PropagateUserSingle).filter(PropagateUserSingle.mid==mid).all()
    
    if len(propagateUsers):
        for p in propagateUsers:
            user = dict()
            if p.user == 'None':
                user['id'] = 0
            else:
                user['id'] = int(p.user)
            user['name'] = p.user_name
            user['location'] = p.location
            user['follower'] = p.follower
            user['friend'] = p.friend
            user['status'] = p.status
            user['description'] = p.description
            user['profile_image_url'] = p.image_url
            blog_info.append(user)       
        return blog_info
    else:
        return blog_info

def readPropagateSinglePart(mid):

    blog_info = []
    propagateSingles = db.session.query(PropagateSinglePart).filter(PropagateSinglePart.mid==mid).all()
    
    if len(propagateSingles):
        for p in propagateSingles:
            user = dict()
            if p.uid == 'None':
                user['id'] = 0
            else:
                user['id'] = int(p.uid)
            user['name'] = p.user_name
            user['repostsCount'] = p.repostsCount
            user['commentsCount'] = p.commentsCount
            user['attitudesCount'] = p.attitudesCount
            user['postDate'] = p.postDate
            user['text'] = p.text
            user['profile_image_url'] = p.image_url
            blog_info.append(user)       
        return blog_info
    else:
        return blog_info

def readPropagateTrendSinglePart(mid):

    blog_info = dict()
    propagateTrendSingles = db.session.query(PropagateTrendSinglePart).filter(PropagateTrendSinglePart.mid==mid).all()
    
    if len(propagateTrendSingles):
        perday_count_list = []
        date_list = []
        for p in propagateTrendSingles:
            perday_count_list.append(p.count)
            date_list.append(p.date)

        blog_info['perday_count'] = perday_count_list
        blog_info['date_list'] = date_list       
        return blog_info
    else:
        return blog_info

def readPropagateWeiboSinglePart(mid):

    blog_info = []
    propagateWeibos = db.session.query(PropagateWeiboSinglePart).filter(PropagateWeiboSinglePart.ori_mid==mid).all()
    
    if len(propagateWeibos):
        
        for p in propagateWeibos:
            user = dict()
            status = dict()
            status['_id'] = p.mid
            user['profile_image_url'] = p.image_url
            status['text'] = p.text
            status['source'] = p.sourcePlatform
            status['created_at'] = p.postDate
            user['id'] = p.uid
            user['name'] = p.user_name
            status['reposts_count'] = p.repostsCount
            status['comments_count'] = p.commentsCount
            status['attitudes_count'] = p.attitudesCount
            blog_info.append({'status':status,'user':user})

        return blog_info
    else:
        return blog_info

def readPropagateSpatialSinglePart(mid):

    propagateSpatials = db.session.query(PropagateSpatialSinglePart).filter(PropagateSpatialSinglePart.mid==mid).all()
    
    if len(propagateSpatials):
        city_count = dict()
        for p in propagateSpatials:
            city_count[p.city] = p.count
        map_data = province_color_map(city_count)
        return map_data
    else:
        city_count = {}
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
        map_data = province_color_map(city_count)
        return map_data

def readIndexSinglePart(mid):

    blog_info = dict()
    propagateTopics = db.session.query(PropagateSinglePart).filter(PropagateSinglePart.mid==mid).all()
    
    if len(propagateTopics):        
        for p in propagateTopics:
            blog_info['persistent_index'] = p.persistent
            blog_info['sudden_index'] = p.sudden
            blog_info['coverage_index'] = p.coverage
            blog_info['media_index'] = p.media
            blog_info['leader_index'] = p.leader       
        return blog_info
    else:
        return blog_info

def readPropagateUserSinglePart(mid):

    blog_info = []
    propagateUsers = db.session.query(PropagateUserSinglePart).filter(PropagateUserSinglePart.mid==mid).all()
    
    if len(propagateUsers):
        for p in propagateUsers:
            user = dict()
            if p.user == 'None':
                user['id'] = 0
            else:
                user['id'] = int(p.user)
            user['name'] = p.user_name
            user['location'] = p.location
            user['follower'] = p.follower
            user['friend'] = p.friend
            user['status'] = p.status
            user['description'] = p.description
            user['profile_image_url'] = p.image_url
            blog_info.append(user)       
        return blog_info
    else:
        return blog_info

def getMaterial(mid, ori_mid, flag):

    blog_info = dict()
    if flag == 0:#整个转发树的微博
        propagateWeibos = db.session.query(PropagateWeiboSingle).filter((PropagateWeiboSingle.ori_mid==ori_mid)&(PropagateWeiboSingle.mid==mid)).all()    
        if len(propagateWeibos):        
            for p in propagateWeibos:
                blog_info['_id'] = p.mid
                blog_info['text'] = p.text
                blog_info['created_at'] = p.postDate
                blog_info['id'] = p.uid
                blog_info['reposts_count'] = p.repostsCount
                blog_info['comments_count'] = p.commentsCount

            return blog_info
        else:
            return blog_info
    else:#以ori_mid为顶点的微博
        propagateWeibos = db.session.query(PropagateWeiboSinglePart).filter((PropagateWeiboSinglePart.ori_mid==ori_mid)&(PropagateWeiboSinglePart.mid==mid)).all()    
        if len(propagateWeibos):        
            for p in propagateWeibos:
                blog_info['_id'] = p.mid
                blog_info['text'] = p.text
                blog_info['created_at'] = p.postDate
                blog_info['id'] = p.uid
                blog_info['reposts_count'] = p.repostsCount
                blog_info['comments_count'] = p.commentsCount

            return blog_info
        else:
            return blog_info

def getMaterialTopic(mid, topic_id):

    blog_info = dict()
    propagateWeibos = db.session.query(PropagateWeibo).filter((PropagateWeibo.mid==mid)&(PropagateWeibo.topic_id==topic_id)).all()    
    if len(propagateWeibos):        
        for p in propagateWeibos:
            blog_info['_id'] = p.mid
            blog_info['text'] = p.text
            blog_info['created_at'] = p.postDate
            blog_info['id'] = p.uid
            blog_info['reposts_count'] = p.repostsCount
            blog_info['comments_count'] = p.commentsCount

        return blog_info
    else:
        return blog_info
