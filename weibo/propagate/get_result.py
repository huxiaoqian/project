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
        return 0

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
            topic_info.append(p.user)       
        return topic_info
    else:
        return topic_info
