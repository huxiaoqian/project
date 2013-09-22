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
search_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)

from flask import Blueprint, url_for, render_template, request, abort, flash, make_response

from weibo.model import *
from weibo.extensions import db

from autocalculate import calculate
from calculate_single import calculate_single,get_user
from calculatetopic import calculate_topic

mod = Blueprint('propagate', __name__, url_prefix='/propagate')

def getFieldTopics():
    field_topics = []
    fields = db.session.query(Field)
    for field in fields:
        field_name = field.fieldName
        topics = db.session.query(Topic).filter_by(field=field)
        topic_names = []
        for topic in topics:
            topic_names.append(topic.topicName)
        field_topics.append({'field_name': field_name, 'topics': topic_names})
    return field_topics

def getHotStatus():
    statuses = db.session.query(HotStatus).order_by(HotStatus.repostsCount.desc()).limit(5)
    status_hot = []             
    for status in statuses:
        uid = status.uid
        user = get_user(uid)
        status_hot.append({'status': status, 'user': user})
    return status_hot

@mod.route("/")
def index():
    field_topics = getFieldTopics()
    status_hot = getHotStatus()
     
    return render_template('propagate/search.html', field_topics=field_topics, status_hot=status_hot)

@mod.route("/showresult/", methods = ["GET","POST"])
def showresult_by_topic():
    # get the input context
    keyword = request.form.get('keyword', "")
    keyuser = request.form.get('keyuser', "")
    beg_time = request.form.get('beg_time', "")
    end_time = request.form.get('end_time', "")
    
    keyword = keyword.strip('@\r\n\t')
    keyuser = keyuser.strip('@\r\n\t')
    beg_time = beg_time.strip('@\r\n\t')
    end_time = end_time.strip('@\r\n\t')

    return_beg_str = beg_time
    return_end_str = end_time
    keyuser_str = keyuser
    
    if keyword == "":
        flash(u'关键字（词）不能为空')
        field_topics = getFieldTopics()
        status_hot = getHotStatus()
        return render_template('propagate/search.html',field_topics = field_topics,status_hot = status_hot)
    if keyuser == "":
        keyuser_str = u'无'
    if beg_time == "":
        beg_time = calendar.timegm(datetime(2013, 1, 1).timetuple())
        return_beg_str = '2013-01-01'
    else:
        beg_time = datetime.strptime(beg_time,"%Y-%m-%d")
        beg_time_year = int(beg_time.year)
        beg_time_month = int(beg_time.month)
        beg_time_day = int(beg_time.day)
        beg_time = calendar.timegm(datetime(beg_time_year,beg_time_month,beg_time_day).timetuple())
    if end_time == "":
        end_time = calendar.timegm(datetime(2013, 1, 7).timetuple())
        return_end_str = '2013-01-07'
    else:
        end_time = datetime.strptime(end_time,"%Y-%m-%d")
        end_time_year = int(end_time.year)
        end_time_month = int(end_time.month)
        end_time_day = int(end_time.day)
        end_time = calendar.timegm(datetime(end_time_year,end_time_month,end_time_day).timetuple())
        
    fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_status','bmiddle_pic','geo','source','attitudes_count'] 
    count, get_results = search_weibo.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['timestamp'], fields=fields_list)

    if count == 0:
        flash(u'您搜索的话题结果为空')
        field_topics = getFieldTopics()
        status_hot = getHotStatus()
        return render_template('propagate/search.html',field_topics = field_topics,status_hot = status_hot)
    else:
        print count
        topic_info = calculate(get_results())
        print 'ok'
        topic_ori_screen_name = topic_info['topic_poster']
        topic_img_url = topic_info['topic_url']
        topic_blog_count = topic_info['blogs_sum']
        topic_blog_ori_count = topic_info['topic_ori_blog_count']
        blog_ori_account = '%10.2f'%(float(topic_blog_ori_count)/topic_blog_count)
        topic_leader_count = topic_info['topic_index']['leader_index']
        topic_ori_date = topic_info['topic_post_date']

        return render_template('propagate/showResult.html',
                                topic_profile_image_url = topic_img_url[0],
                                topic_ori_screen_name = topic_ori_screen_name,
                                blog_rel_count = topic_blog_count,
                                blog_ori_count = topic_blog_ori_count,
                                blog_ori_account = blog_ori_account,
                                topic_leader_count = topic_leader_count,
                                topic_ori_date = topic_ori_date,
                                keyword=keyword, keyuser=keyuser, beg_time=beg_time, end_time=end_time,
                                return_beg_str=return_beg_str, return_end_str=return_end_str, keyuser_str=keyuser_str
        )

@mod.route("/topic_ajax_trend/", methods = ["GET","POST"])
def topic_ajax_trend():
    if request.method == "GET":
        return render_template('propagate/ajax/topic_trend.html')
    else:
        keyword = request.form.get('keyword', "")
        keyuser = request.form.get('keyuser', "")
        beg_time = int(request.form.get('beg_time', ""))
        end_time = int(request.form.get('end_time', ""))
    
        fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_status','bmiddle_pic','geo','source','attitudes_count'] 
        count, get_results = search_weibo.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['timestamp'], fields=fields_list)

        topic_info = calculate(get_results())
        perday_blog_count = topic_info['perday_count_list']
        date_list = topic_info['date_list']
        date_list = [int(time.mktime(d.timetuple()))*1000 for d in date_list]

        return json.dumps({'perday_blog_count': zip(date_list, perday_blog_count)})

@mod.route("/topic_ajax_weibos/")
def topic_ajax_weibos():
    if request.method == "GET":
        keyword = request.args.get('keyword', "")
        keyuser = request.args.get('keyuser', "")
        beg_time = int(request.args.get('beg_time', ""))
        end_time = int(request.args.get('end_time', ""))
    
        fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_status','bmiddle_pic','geo','source','attitudes_count'] 
        count, get_results = search_weibo.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['timestamp'], fields=fields_list)

        topic_info = calculate(get_results())
        blog_rel_list = topic_info['topic_rel_blog'][:5]

        return render_template('propagate/ajax/topic_weibos.html', blog_rel_list= blog_rel_list)

@mod.route("/topic_ajax_spatial/", methods=['GET', 'POST'])
def topic_ajax_spatial():
    if request.method == "GET":
        return render_template('propagate/ajax/topic_spatial.html')
    else:
        keyword = request.form.get('keyword', "")
        keyuser = request.form.get('keyuser', "")
        beg_time = int(request.form.get('beg_time', ""))
        end_time = int(request.form.get('end_time', ""))
    
        fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_status','bmiddle_pic','geo','source','attitudes_count'] 
        count, get_results = search_weibo.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['timestamp'], fields=fields_list)

        topic_info = calculate(get_results())
        topic_area_list = topic_info['geo']

        return json.dumps({'map_data': topic_area_list})

@mod.route("/topic_ajax_stat/")
def topic_ajax_stat():
    if request.method == 'GET':
        keyword = request.args.get('keyword', "")
        keyuser = request.args.get('keyuser', "")
        beg_time = int(request.args.get('beg_time', ""))
        end_time = int(request.args.get('end_time', ""))
    
        fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_status','bmiddle_pic','geo','source','attitudes_count'] 
        count, get_results = search_weibo.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['timestamp'], fields=fields_list)

        topic_info = calculate(get_results())

        topic_persistent_count = topic_info['topic_index']['persistent_index']
        topic_sudden_count = topic_info['topic_index']['sudden_index']
        topic_coverage_count = topic_info['topic_index']['coverage_index']
        topic_media_count = topic_info['topic_index']['media_index']
        topic_leader_count = topic_info['topic_index']['leader_index']

        return render_template('propagate/ajax/topic_stat.html',
                                topic_persistent_count = topic_persistent_count,
                                topic_sudden_count = topic_sudden_count,
                                topic_coverage_count = topic_coverage_count,
                                topic_media_count = topic_media_count,
                                topic_leader_count = topic_leader_count
        )

@mod.route("/topic_ajax_path/", methods=['GET', 'POST'])
def topic_ajax_path():
    if request.method == "GET":
        return render_template('propagate/ajax/topic_retweetpath.html')

@mod.route("/topic_ajax_userfield/", methods=['GET', 'POST'])
def topic_ajax_userfield():
    if request.method == "GET":
        keyword = request.args.get('keyword', "")
        keyuser = request.args.get('keyuser', "")
        beg_time = int(request.args.get('beg_time', ""))
        end_time = int(request.args.get('end_time', ""))
    
        fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_status','bmiddle_pic','geo','source','attitudes_count'] 
        count, get_results = search_weibo.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['timestamp'], fields=fields_list)

        topic_info = calculate(get_results())
        topic_key_user_list = topic_info['topic_leader']

        return render_template('propagate/ajax/topic_userfield.html',  topic_key_user_list= topic_key_user_list)
    else:
        pass

@mod.route("/showresult_single/<mid>/", methods = ["GET","POST"])
def single_analysis(mid):
    mid = int(mid)
    blog_info = calculate_single(mid)
                                 
    blog_img_url = blog_info['user']['profile_image_url']
    blog_date_list = blog_info['datelist']

    bloger_name = blog_info['user']['name']
    blog_reposts_count = blog_info['status']['repostsCount']
    blog_comments_count = blog_info['status']['commentsCount']
    blog_attitudes_count = blog_info['status']['attitudesCount']
    blog_time = blog_info['status']['postDate']

    return render_template('propagate/showResult_single.html', 
                           mid=mid,
                           tar_profile_image_url = blog_img_url,
                           tar_screen_name = bloger_name,
                           tar_repost_count = blog_reposts_count,
                           tar_comments_count = blog_comments_count,
                           tar_attitudes_count = blog_attitudes_count,
                           tar_post_date = blog_time,
                           blog_date_list = blog_date_list,
                           )

@mod.route("/single_ajax_trend/", methods = ["GET","POST"])
def single_ajax_trend():
    if request.method == "GET":
        return render_template('propagate/ajax/single_trend.html')
    else:
        mid = int(request.form.get('mid', ""))
        blog_info = calculate_single(mid)
        perday_repost_count = blog_info['perday_count']
        blog_date_list = blog_info['datelist']
        date_list = [int(time.mktime(d.timetuple()))*1000 for d in blog_date_list]

        return json.dumps({'perday_blog_count': zip(date_list, perday_repost_count)})

@mod.route("/single_ajax_weibos/")
def single_ajax_weibos():
    if request.method == "GET":
        mid = int(request.args.get('mid', ""))
        blog_info = calculate_single(mid)

        bloger_name = blog_info['user']['name']
        blog_reposts_count = blog_info['status']['repostsCount']
        blog_comments_count = blog_info['status']['commentsCount']
        blog_attitudes_count = blog_info['status']['attitudesCount']
        blog_img_url = blog_info['user']['profile_image_url']
    
        blog_time = blog_info['status']['postDate']
        blog_text = blog_info['status']['text']
        blog_source = blog_info['status']['sourcePlatform']
        blog_id = blog_info['status']['id']

        return render_template('propagate/ajax/single_weibos.html', 
                               tar_profile_image_url = blog_img_url,
                               tar_screen_name = bloger_name,
                               tar_repost_count = blog_reposts_count,
                               tar_comments_count = blog_comments_count,
                               tar_attitudes_count = blog_attitudes_count,
                               tar_post_date = blog_time,
                               tar_text = blog_text,
                               tar_source = blog_source,
                               tar_id = blog_id
                              )

@mod.route("/single_ajax_spatial/", methods=['GET', 'POST'])
def single_ajax_spatial():
    if request.method == "GET":
        return render_template('propagate/ajax/single_spatial.html')
    else:
        mid = int(request.form.get('mid', ""))
        blog_info = calculate_single(mid)
        area_list = blog_info['geo']

        return json.dumps({'map_data': area_list})

@mod.route("/single_ajax_stat/")
def single_ajax_stat():
    if request.method == 'GET':
        mid = int(request.args.get('mid', ""))
        blog_info = calculate_single(mid)

        tar_persistent_count = blog_info['persistent_index']
        tar_sudden_count = blog_info['sudden_index']
        tar_coverage_count = blog_info['coverage_index']
        tar_media_count = blog_info['media_index']
        tar_leader_count = blog_info['leader_index']
    
        return render_template('propagate/ajax/single_stat.html',
                                tar_persistent_count = tar_persistent_count,
                                tar_sudden_count = tar_sudden_count,
                                tar_coverage_count = tar_coverage_count,
                                tar_media_count = tar_media_count,
                                tar_leader_count = tar_leader_count
        )

@mod.route("/single_ajax_path/", methods=['GET', 'POST'])
def single_ajax_path():
    if request.method == "GET":
        return render_template('propagate/ajax/single_retweetpath.html')

@mod.route("/single_ajax_userfield/", methods=['GET', 'POST'])
def single_ajax_userfield():
    if request.method == "GET":
        mid = int(request.args.get('mid', ""))
        blog_info = calculate_single(mid)

        repost_bloger = blog_info['repost_users']
        blog_key_user_list = repost_bloger

        return render_template('propagate/ajax/single_userfield.html',  blog_key_user_list=blog_key_user_list)
    else:
        pass

@mod.route("/add_material", methods = ["GET","POST"])
def add_material():
    result = 'Right'
    mid = request.form['mid']
    mid = int(mid)
    blog_info = calculate_single(mid)
                                 
    blog_reposts_count = blog_info['status']['repostsCount']
    blog_comments_count = blog_info['status']['commentsCount']
    blog_time = blog_info['status']['postDate']
    blog_text = blog_info['status']['text']
    blog_id = blog_info['status']['id']
    bloger_ids = db.session.query(HotStatus).filter(HotStatus.id==blog_id).all()
    ma_ids = db.session.query(M_Weibo).filter(M_Weibo.weibo_id==blog_id).all()
    if len(ma_ids):
        result = 'Wrong'
    else:
        for bloger_id in bloger_ids:
            new_item = M_Weibo(weibo_id=blog_id,text=blog_text.encode('utf-8'),repostsCount=blog_reposts_count,commentsCount=blog_comments_count,postDate=blog_time,uid=bloger_id.uid)
            db.session.add(new_item)
            db.session.commit()
    return json.dumps(result)

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
