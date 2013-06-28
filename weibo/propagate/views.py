# -*- coding: utf-8 -*-

import os
import sys
import time
try:
    import simplejosn as json
except ImportError:
    import json
import re
import calendar
import time
from datetime import date
from datetime import datetime

from xapian_weibo.xapian_backend import XapianSearch

from flask import Blueprint, url_for, render_template, request, abort, flash, make_response

from weibo.model import *
from weibo.extensions import db

from autocalculate import calculate
from calculate_single import calculate_single,get_user
from calculatetopic import calculate_topic

mod = Blueprint('propagate', __name__, url_prefix='/propagate')

@mod.route("/")
def index():
    field_topics = []
    fields = db.session.query(Field)
    for field in fields:
        field_name = field.fieldName
        topics = db.session.query(Topic).filter_by(field=field)
        topic_names = []
        for topic in topics:
            topic_names.append(topic.topicName)
        field_topics.append({'field_name': field_name, 'topics': topic_names})

    statuses = db.session.query(HotStatus).order_by(HotStatus.repostsCount.desc()).limit(5)
    status_hot = []             
    for status in statuses:
        uid = status.uid
        user = get_user(uid)
        status_hot.append({'status': status, 'user': user}) 
    return render_template('propagate/search.html',field_topics = field_topics,status_hot = status_hot)

@mod.route("/showResult/", methods = ["GET","POST"])
def showresult_by_topic():
    field_topics = []
    fields = db.session.query(Field)
    for field in fields:
        field_name = field.fieldName
        topics = db.session.query(Topic).filter_by(field=field)
        topic_names = []
        for topic in topics:
            topic_names.append(topic.topicName)
        field_topics.append({'field_name': field_name, 'topics': topic_names})

    statuses = db.session.query(HotStatus).order_by(HotStatus.repostsCount.desc()).limit(5)
    status_hot = []             
    for status in statuses:
        uid = status.uid
        user = get_user(uid)
        status_hot.append({'status': status, 'user': user})

# get the input context
    keyword = request.form['keyword']
    keyuser = request.form['keyuser']
    beg_time = request.form['beg_time']
    end_time = request.form['end_time']
    
    keyword = keyword.strip('@\r\n\t')
    keyuser = keyuser.strip('@\r\n\t')
    beg_time = beg_time.strip('@\r\n\t')
    end_time = end_time.strip('@\r\n\t')
    
    if keyword == "":
        flash(u'关键字（词）不能为空')
        return render_template('propagate/search.html',field_topics = field_topics,status_hot = status_hot)
    if keyuser == "":
        pass
    
    if beg_time == "":
        beg_time = calendar.timegm(datetime(2011, 1, 1).timetuple())
    else:
        beg_time = datetime.strptime(beg_time,"%Y-%m-%d")
        beg_time_year = int(beg_time.year)
        beg_time_month = int(beg_time.month)
        beg_time_day = int(beg_time.day)
        beg_time = calendar.timegm(datetime(beg_time_year,beg_time_month,beg_time_day).timetuple())
    if end_time == "":
        today = date.today()
        end_time = calendar.timegm(datetime(int(today.year), int(today.month), int(today.day)).timetuple())
    else:
        end_time = datetime.strptime(end_time,"%Y-%m-%d")
        end_time_year = int(end_time.year)
        end_time_month = int(end_time.month)
        end_time_day = int(end_time.day)
        end_time = calendar.timegm(datetime(end_time_year,end_time_month,end_time_day).timetuple())
        
    s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
    count, get_results = s.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['timestamp'], fields=['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_status','bmiddle_pic','geo','source','attitudes_count'])

    if count == 0:
        return render_template('propagate/search.html',field_topics = field_topics,status_hot = status_hot)
    else:
        print count
        topic_info = calculate(get_results())
        print 'ok'
        topic_ori_screen_name = topic_info['topic_poster']
        topic_ori_date = topic_info['topic_post_date']
        topic_leaders_count = topic_info['topic_leader_count']
        topic_participants_count = topic_info['topic_participents']
        topic_blog_count = topic_info['blogs_sum']
        topic_blog_ori_count = topic_info['topic_ori_blog_count']
        blog_ori_account = '%10.2f'%(float(topic_blog_ori_count)/topic_blog_count)
        topic_img_url = topic_info['topic_url']

        perday_blog_count = topic_info['perday_count_list']
        date_list = topic_info['date_list']
        blog_rel_list = topic_info['topic_rel_blog']

        topic_area_list = topic_info['geo']

        topic_key_user_list = topic_info['topic_leader']

        topic_working_list = topic_info['topic_working_list']
        topic_working_count = topic_info['topic_working_count']

        topic_persistent_count = topic_info['topic_index']['persistent_index']
        topic_sudden_count = topic_info['topic_index']['sudden_index']
        topic_coverage_count = topic_info['topic_index']['coverage_index']
        topic_media_count = topic_info['topic_index']['media_index']
        topic_leader_count = topic_info['topic_index']['leader_index']

        #date_list = [d.strftime('%Y-%m-%d') for d in date_list]

        date_list = [int(time.mktime(d.timetuple()))*1000 for d in date_list]

        r = int(request.form.get('r', 0))
        if not r:
            if request.method == 'POST':
                return json.dumps({'perday_blog_count': zip(date_list, perday_blog_count), 'map_data': topic_area_list})
        return render_template('propagate/showResult.html',
                                topic_profile_image_url = topic_img_url[0],
                                topic_ori_screen_name = topic_ori_screen_name,
                                blog_rel_count = topic_blog_count,
                                blog_ori_count = topic_blog_ori_count,
                                blog_ori_account = blog_ori_account,
                                topic_leader_count = topic_leader_count,
                                topic_ori_date = topic_ori_date,
                                blog_rel_list = blog_rel_list[:5],
                                perday_blog_count = json.dumps(perday_blog_count),
                                date_list = json.dumps(date_list),
                                topic_area_list = topic_area_list,
                                topic_persistent_count = topic_persistent_count,
                                topic_sudden_count = topic_sudden_count,
                                topic_coverage_count = topic_coverage_count,
                                topic_media_count = topic_media_count,
                                topic_working_list = topic_working_list,
                                topic_working_count = topic_working_count,
                                topic_key_user_list = topic_key_user_list,
                                keyword=keyword, keyuser=keyuser, beg_time=request.form['beg_time'], end_time=request.form['end_time']
                                )
        
@mod.route("/showTopicResult/<topic>/")
def show_tpoic(topic):
    kw = topic
    keyword = kw
    keyuser = ""
    print kw
    topic_info = calculate_topic(kw)
    gt = topic_info['gt']
    lt = topic_info['lt']
    topic_ori_screen_name = topic_info['topic_poster']
    topic_ori_date = topic_info['topic_post_date']
    topic_leaders_count = topic_info['topic_leader_count']
    topic_participants_count = topic_info['topic_participents']
    topic_blog_count = topic_info['blogs_sum']
    topic_blog_ori_count = topic_info['topic_ori_blog_count']
    blog_ori_account = topic_blog_ori_count / topic_blog_count
    topic_img_url = topic_info['topic_url']

    perday_blog_count = topic_info['perday_count_list']
    date_list = topic_info['date_list']
    blog_rel_list = topic_info['topic_rel_blog']

    topic_area_list = topic_info['geo']

    topic_key_user_list = topic_info['topic_leader']

    topic_working_list = topic_info['topic_working_list']
    topic_working_count = topic_info['topic_working_count']

    topic_persistent_count = topic_info['topic_index']['persistent_index']
    topic_sudden_count = topic_info['topic_index']['sudden_index']
    topic_coverage_count = topic_info['topic_index']['coverage_index']
    topic_media_count = topic_info['topic_index']['media_index']
    topic_leader_count = topic_info['topic_index']['leader_index']


    date_list = [int(time.mktime(d.timetuple()))*1000 for d in date_list]
    r = int(request.form.get('r', 0))
    if not r:
        if request.method == 'POST':
            return json.dumps({'perday_blog_count': zip(date_list, perday_blog_count), 'map_data': topic_area_list})
    return render_template('propagate/showResult.html',
                            topic_profile_image_url = topic_img_url[0],
                            topic_ori_screen_name = topic_ori_screen_name,
                            blog_rel_count = topic_blog_count,
                            blog_ori_count = topic_blog_ori_count,
                            blog_ori_account = blog_ori_account,
                            topic_leader_count = topic_leader_count,
                            topic_ori_date = topic_ori_date,
                            blog_rel_list = blog_rel_list[:5],
                            perday_blog_count = json.dumps(perday_blog_count),
                            date_list = json.dumps(date_list),
                            topic_area_list = topic_area_list,
                            topic_persistent_count = topic_persistent_count,
                            topic_sudden_count = topic_sudden_count,
                            topic_coverage_count = topic_coverage_count,
                            topic_media_count = topic_media_count,
                            topic_working_list = topic_working_list,
                            topic_working_count = topic_working_count,
                            topic_key_user_list = topic_key_user_list,
                            keyword=keyword, keyuser=keyuser, beg_time = gt, end_time= lt
                            )

@mod.route("/showResult_single/<mid>/", methods = ["GET","POST"])
def single_analysis(mid):
    mid = int(mid)
    blog_info = calculate_single(mid)
                                 
    blog_img_url = blog_info['user']['profile_image_url']
    bloger_name = blog_info['user']['name']
    blog_reposts_count = blog_info['status']['repostsCount']
    blog_comments_count = blog_info['status']['commentsCount']
    blog_attitudes_count = blog_info['status']['attitudesCount']
    blog_time = blog_info['status']['postDate']
    blog_text = blog_info['status']['text']
    blog_source = blog_info['status']['sourcePlatform']
    blog_id = blog_info['status']['id']
    repost_bloger = blog_info['repost_users']
    blog_date_list = blog_info['datelist']
    perday_repost_count = blog_info['perday_count']
    tar_persistent_count = blog_info['persistent_index']
    tar_sudden_count = blog_info['sudden_index']
    tar_coverage_count = blog_info['coverage_index']
    tar_media_count = blog_info['media_index']
    tar_leader_count = blog_info['leader_index']
    area_list = blog_info['geo']
    
    
    date_list = [int(time.mktime(d.timetuple()))*1000 for d in blog_date_list]
    if request.method == 'POST':
        return json.dumps({'perday_blog_count': zip(date_list, perday_repost_count), 'map_data': area_list})

    return render_template('propagate/showResult_single.html', 
                           mid=mid,
                           tar_profile_image_url = blog_img_url,
                           tar_screen_name = bloger_name,
                           tar_repost_count = blog_reposts_count,
                           tar_comments_count = blog_comments_count,
                           tar_attitudes_count = blog_attitudes_count,
                           tar_post_date = blog_time,
                           tar_text = blog_text,
                           tar_source = blog_source,
                           tar_id = blog_id,
                           blog_date_list = blog_date_list,
                           perday_repost_count = perday_repost_count,
                           tar_persistent_count = tar_persistent_count,
                           tar_sudden_count = tar_sudden_count,
                           tar_coverage_count = tar_coverage_count,
                           tar_media_count = tar_media_count,
                           tar_leader_count = tar_leader_count,
                           blog_key_user_list = repost_bloger
                           )

@mod.route("/topics")
def topics():
    field_topics = []
    fields = db.session.query(Field)
    for field in fields:
        field_name = field.fieldName
        topics = db.session.query(Topic).filter_by(field=field)
        topic_names = []
        for topic in topics:
            topic_names.append(topic.topicName)
        field_topics.append({'field_name': field_name, 'topics': topic_names})
    return render_template('propagate/topics.html',field_topics = field_topics)

@mod.route("/hot_status")
def hot_status():
    statuses = db.session.query(HotStatus).order_by(HotStatus.repostsCount.desc()).limit(100)
    status_hot = []             
    for status in statuses:
        uid = status.uid
        user = get_user(uid)
        status_hot.append({'status': status, 'user': user}) 
    return render_template('propagate/hot_status.html',status_hot = status_hot)
