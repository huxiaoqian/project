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
import redis

from xapian_weibo.xapian_backend import XapianSearch
from weibo.global_config import xapian_search_user, xapian_search_weibo, xapian_search_domain, LEVELDBPATH, \
                                fields_value, fields_id, emotions_zh_kv, emotions_kv
from xapian_config import beg_y,beg_m,beg_d,end_y,end_m,end_d

from flask import Blueprint, url_for, render_template, request, abort, flash, make_response, session, redirect

from weibo.model import *
from weibo.extensions import db
import weibo.model
import json

from autocalculate import calculate
from calculate_single import calculate_single,get_user
from calculatetopic import calculate_topic
from history import _all_history, _add_history, _search_history
from history_weibo import _all_history_weibo, _add_history_weibo, _search_history_weibo
from get_result import *
from xapian_weibo.xapian_backend import XapianSearch

XAPIAN_FIRST_DATE = '20130901'
XAPIAN_LAST_DATE = '20130930'

sys.path.append('./weibo/propagate/graph')
from tree import *
from forest import *
from url2mid import *
sys.path.append('./weibo/profile')
from utils import *

path = '/home/ubuntu12/dev/data/stub/master_timeline_weibo_'

fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology','aboard','university', 'homeadmin', 'abroadadmin', 'homemedia', 'abroadmedia', 'folkorg', 'lawyer', 'politician', 'mediaworker', 'activer', 'grassroot', 'other']
fields_id = {'culture':1, 'education':2, 'entertainment':3, 'fashion':4, 'finance':5, 'media':6, 'sports':7, 'technology':8,'aboard':9,'university':10, 'homeadmin':11, 'abroadadmin':12, 'homemedia':13, 'abroadmedia':14, 'folkorg':15, 'lawyer':16, 'politician':17, 'mediaworker':18, 'activer':19, 'grassroot':20}
month_value = {'January':1, 'February':2, 'March':3, 'April':4, 'May':5, 'June':6, 'July':7, 'August':8, 'September':9, 'October':10, 'November':11, 'December':12}

USER_DOMAIN = "user_domain"
REDIS_HOST = '192.168.2.11'
REDIS_PORT = 6379

mod = Blueprint('propagate', __name__, url_prefix='/propagate')

def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)

def user2domain(uid):
    global_r0 = _default_redis()
    domainid = global_r0.hget(USER_DOMAIN, str(uid))
    if not domainid:
        domainid = -1 # not taged label
    
    return int(domainid)

def ts2datetimestr(ts):
    return time.strftime('%Y%m%d', time.localtime(ts))

def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d %H:%M:%S'))

def date2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def user_status(uid):
    old_items = db.session.query(KnowledgeList).filter(KnowledgeList.kID==uid).all()
    if len(old_items):
        return 1
    else:
        return 0

def _utf_encode(s):
    if isinstance(s, str):
        return s
    else:
        return s.encode('utf-8')


def _utf_decode(s):
    if isinstance(s, str):
        return s.decode('utf-8')
    else:
        return s

def _default_time_zone():
    '''默认时间段为最新一周
    '''

    end_ts = time.time()
    start_ts = end_ts - 7 * 24 * 3600

    return start_ts, end_ts


def _time_zone(stri):
    dates = stri.split(' - ')
    tslist = []

    for date in dates:
        month_day, year = date.split(',')
        month, day = month_day.split('月 ')
        year = int(year)
        month = int(month)
        day = int(day)
        ts = str(year)+'-'+str(month)+'-'+str(day)
        tslist.append(ts)

    start_ts = tslist[0]
    end_ts = tslist[1]

    return start_ts, end_ts

def _time_yuan(stri):
    dates = stri.split(' - ')
    tslist = []

    for date in dates:
        month_day, year = date.split(',')
        month, day_d = month_day.split('月 ')
        year = int(year)
        month = int(month)
        day, d = day_d.split('日')
        day = int(day)
        ts = str(year)+'-'+str(month)+'-'+str(day)
        tslist.append(ts)

    start_ts = tslist[0]
    end_ts = tslist[1]

    return start_ts, end_ts


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
    Day = 24*3600
    days = (int(end_time) - int(start_time)) / Day

    for i in range(0, days):
        _ts = start_time + i * Day
        xapian_date_list.append(ts2datetimestr(_ts))

    statuses_search = getXapianWeiboByDuration(xapian_date_list)
    return statuses_search

def timechange(time_str):
    year,month,day = time_str.split('-')
    return str(month)+'月 '+str(day)+','+str(year)

def date2timestr(time):
    time_ts = datetime2ts(time)
    end_ts = time_ts + 24*3600
    time_str = str(ts2datetime(time_ts))
    end_str = str(ts2datetime(end_ts))
    beg = timechange(time_str)
    end = timechange(end_str)
    print beg,end
    return str(beg)+' - '+str(end)

def fieldsEn2Zh(name):
    if name == 'finance':
        return '财经'
    if name == 'media':
        return '媒体'
    if name == 'culture':
        return '文化'
    if name == 'technology':
        return '科技'
    if name == 'entertainment':
        return '娱乐'
    if name == 'education':
        return '教育'
    if name == 'fashion':
        return '时尚'
    if name == 'sports':
        return '体育'
    if name == 'oversea':
        return '境外'
    if name == 'university':
        return '高校微博'
    if name == 'homeadmin':
        return '境内机构'
    if name == 'abroadadmin':
        return '境外机构'
    if name == 'homemedia':
        return '境内媒体'
    if name == 'abroadmedia':
        return '境外媒体'
    if name == 'folkorg':
        return '民间组织'
    if name == 'lawyer':
        return '律师'
    if name == 'politician':
        return '政府官员'
    if name == 'mediaworker':
        return '媒体人士'
    if name == 'activer':
        return '活跃人士'
    if name == 'grassroot':
        return '草根'
    if name == 'other':
        return '其他'


@mod.route('/log_in', methods=['GET','POST'])
def log_in():
    session['logged_in'] = request.form['log_in']
    session['user'] = request.form['user']
    if 'logged_in' in session and session['logged_in']:
        return json.dumps('Right')
    else:
        return json.dumps('Wrong')

@mod.route("/")
def index():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            mid = request.args.get('mid', '')
            time = str(request.args.get('time', ''))
            if mid:
                time_str = date2timestr(time)
                time_str = _utf_decode(time_str)
                return render_template('propagate/search.html',mid=mid,time_str=time_str)
            else:
                return render_template('propagate/search.html')
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
     
                        mid = request.args.get('mid', '')
                        time = str(request.args.get('time', ''))
                        if mid:
                            time_str = date2timestr(time)
                            time_str = _utf_decode(time_str)
                            return render_template('propagate/search.html',mid=mid,time_str=time_str)
                        else:
                            return render_template('propagate/search.html')
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/showresult/", methods = ["GET","POST"])
def showresult_by_topic():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
    # get the input context
            keyword = request.args.get('keyword', '')
            dur_time = request.args.get('time', '')

            dur_time = _utf_encode(dur_time)
            if not dur_time or dur_time == '':
                beg_time, end_time = _default_time_zone()
            else:
                dur_time = _utf_encode(dur_time)
                beg_time, end_time = _time_yuan(dur_time)
            print type(keyword),keyword

            return_beg_str = beg_time
            return_end_str = end_time

            if beg_time == "":
                beg_time = calendar.timegm(datetime(beg_y, beg_m, beg_d).timetuple())
                return_beg_str = str(beg_y)+'-'+str(beg_m)+'-'+str(beg_d)
            else:
                beg_time = datetime.strptime(beg_time,"%Y-%m-%d")
                beg_time_year = int(beg_time.year)
                beg_time_month = int(beg_time.month)
                beg_time_day = int(beg_time.day)
                beg_time = calendar.timegm(datetime(beg_time_year,beg_time_month,beg_time_day).timetuple())
            if end_time == "":
                end_time = calendar.timegm(datetime(end_y, end_m, end_d).timetuple())
                return_end_str = str(end_y)+'-'+str(end_m)+'-'+str(end_d)
            else:
                end_time = datetime.strptime(end_time,"%Y-%m-%d")
                end_time_year = int(end_time.year)
                end_time_month = int(end_time.month)
                end_time_day = int(end_time.day)
                end_time = calendar.timegm(datetime(end_time_year,end_time_month,end_time_day).timetuple())

            beg_date = ts2datetime(beg_time)
            end_date = ts2datetime(end_time)

            topic_info = readPropagateTopic(keyword,beg_date,end_date)
            
            if not topic_info:
                flash(u'您搜索的话题结果为空')
                return redirect('/propagate/')
            else:

                topic_ori_screen_name = topic_info['topic_poster']
                topic_img_url = topic_info['topic_url']
                topic_blog_count = topic_info['blogs_sum']
                topic_blog_ori_count = topic_info['topic_ori_blog_count']
                blog_ori_account = '%10.2f'%(float(topic_blog_ori_count)/topic_blog_count)
                topic_leader_count = topic_info['leader_index']
                topic_ori_date = topic_info['topic_post_date']
                topic_id = topic_info['id']
                if len(topic_img_url):
                    topic_profile_image_url = topic_img_url[0]
                else:
                    topic_profile_image_url = ''
                return render_template('propagate/showResult.html',
                                        topic_profile_image_url = topic_profile_image_url,
                                        topic_ori_screen_name = topic_ori_screen_name,
                                        blog_rel_count = topic_blog_count,
                                        blog_ori_count = topic_blog_ori_count,
                                        blog_ori_account = blog_ori_account,
                                        topic_leader_count = topic_leader_count,
                                        topic_ori_date = topic_ori_date,
                                        keyword=keyword, topic_id=topic_id, beg_time=beg_time, end_time=end_time,
                                        return_beg_str=return_beg_str, return_end_str=return_end_str
                )
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        keyword = request.args.get('keyword', '')
                        dur_time = request.args.get('time', '')

                        dur_time = _utf_encode(dur_time)
                        if not dur_time or dur_time == '':
                            beg_time, end_time = _default_time_zone()
                        else:
                            dur_time = _utf_encode(dur_time)
                            beg_time, end_time = _time_yuan(dur_time)

                        return_beg_str = beg_time
                        return_end_str = end_time

                        if beg_time == "":
                            beg_time = calendar.timegm(datetime(beg_y, beg_m, beg_d).timetuple())
                            return_beg_str = str(beg_y)+'-'+str(beg_m)+'-'+str(beg_d)
                        else:
                            beg_time = datetime.strptime(beg_time,"%Y-%m-%d")
                            beg_time_year = int(beg_time.year)
                            beg_time_month = int(beg_time.month)
                            beg_time_day = int(beg_time.day)
                            beg_time = calendar.timegm(datetime(beg_time_year,beg_time_month,beg_time_day).timetuple())
                        if end_time == "":
                            end_time = calendar.timegm(datetime(end_y, end_m, end_d).timetuple())
                            return_end_str = str(end_y)+'-'+str(end_m)+'-'+str(end_d)
                        else:
                            end_time = datetime.strptime(end_time,"%Y-%m-%d")
                            end_time_year = int(end_time.year)
                            end_time_month = int(end_time.month)
                            end_time_day = int(end_time.day)
                            end_time = calendar.timegm(datetime(end_time_year,end_time_month,end_time_day).timetuple())

                        beg_date = ts2datetime(beg_time)
                        end_date = ts2datetime(end_time)

                        topic_info = readPropagateTopic(keyword,beg_date,end_date)
            
                        if not topic_info:
                            flash(u'您搜索的话题结果为空')
                            return redirect('/propagate/')
                        else:

                            topic_ori_screen_name = topic_info['topic_poster']
                            topic_img_url = topic_info['topic_url']
                            topic_blog_count = topic_info['blogs_sum']
                            topic_blog_ori_count = topic_info['topic_ori_blog_count']
                            blog_ori_account = '%10.2f'%(float(topic_blog_ori_count)/topic_blog_count)
                            topic_leader_count = topic_info['leader_index']
                            topic_ori_date = topic_info['topic_post_date']
                            topic_id = topic_info['id']
                            if len(topic_img_url):
                                topic_profile_image_url = topic_img_url[0]
                            else:
                                topic_profile_image_url = ''
                            return render_template('propagate/showResult.html',
                                                    topic_profile_image_url = topic_profile_image_url,
                                                    topic_ori_screen_name = topic_ori_screen_name,
                                                    blog_rel_count = topic_blog_count,
                                                    blog_ori_count = topic_blog_ori_count,
                                                    blog_ori_account = blog_ori_account,
                                                    topic_leader_count = topic_leader_count,
                                                    topic_ori_date = topic_ori_date,
                                                    keyword=keyword, topic_id=topic_id, beg_time=beg_time, end_time=end_time,
                                                    return_beg_str=return_beg_str, return_end_str=return_end_str
                            )
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/topic_ajax_trend/", methods = ["GET","POST"])
def topic_ajax_trend():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == "GET":
                return render_template('propagate/ajax/topic_trend.html')
            else:
                keyword = request.form.get('topic_id', "")              

                topic_info = readPropagateTrend(keyword)
                perday_blog_count = topic_info['perday_count_list']
                date_list = topic_info['date_list']
                date_list = [int(time.mktime(d.timetuple())+24*3600)*1000 for d in date_list]
                return json.dumps({'perday_blog_count': zip(date_list, perday_blog_count)})
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            return render_template('propagate/ajax/topic_trend.html')
                        else:
                            keyword = request.form.get('topic_id', "")              

                            topic_info = readPropagateTrend(keyword)
                            perday_blog_count = topic_info['perday_count_list']
                            date_list = topic_info['date_list']
                            date_list = [int(time.mktime(d.timetuple())+24*3600)*1000 for d in date_list]
                            return json.dumps({'perday_blog_count': zip(date_list, perday_blog_count)})
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/topic_ajax_weibos/")
def topic_ajax_weibos():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == "GET":
                keyword = request.args.get('topic_id', "")
                beg_time = int(request.args.get('beg_time', ""))
                end_time = int(request.args.get('end_time', ""))
                topic_info = readPropagateWeibo(keyword)

                return render_template('propagate/ajax/topic_weibos.html', topic_info = topic_info, beg_time = beg_time, end_time = end_time, topic_id = keyword)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            keyword = request.args.get('topic_id', "")
                            beg_time = int(request.args.get('beg_time', ""))
                            end_time = int(request.args.get('end_time', ""))
                            topic_info = readPropagateWeibo(keyword)

                            return render_template('propagate/ajax/topic_weibos.html', topic_info = topic_info, beg_time = beg_time, end_time = end_time, topic_id = keyword)
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/topic_ajax_spatial/", methods=['GET', 'POST'])
def topic_ajax_spatial():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == "GET":
                return render_template('propagate/ajax/topic_spatial.html')
            else:
                keyword = request.form.get('topic_id', "")

                topic_info = readPropagateSpatial(keyword)

                return json.dumps({'map_data': topic_info})
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            return render_template('propagate/ajax/topic_spatial.html')
                        else:
                            keyword = request.form.get('topic_id', "")

                            topic_info = readPropagateSpatial(keyword)

                            return json.dumps({'map_data': topic_info})
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/topic_ajax_stat/")
def topic_ajax_stat():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == 'GET':
                keyword = request.args.get('topic_id', "")

                topic_info = readIndex(keyword)

                topic_persistent_count = topic_info['persistent_index']
                topic_sudden_count = topic_info['sudden_index']
                topic_coverage_count = topic_info['coverage_index']
                topic_media_count = topic_info['media_index']
                topic_leader_count = topic_info['leader_index']

                return render_template('propagate/ajax/topic_stat.html',
                                        topic_persistent_count = topic_persistent_count,
                                        topic_sudden_count = topic_sudden_count,
                                        topic_coverage_count = topic_coverage_count,
                                        topic_media_count = topic_media_count,
                                        topic_leader_count = topic_leader_count
                )
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == 'GET':
                            keyword = request.args.get('topic_id', "")

                            topic_info = readIndex(keyword)

                            topic_persistent_count = topic_info['persistent_index']
                            topic_sudden_count = topic_info['sudden_index']
                            topic_coverage_count = topic_info['coverage_index']
                            topic_media_count = topic_info['media_index']
                            topic_leader_count = topic_info['leader_index']

                            return render_template('propagate/ajax/topic_stat.html',
                                                    topic_persistent_count = topic_persistent_count,
                                                    topic_sudden_count = topic_sudden_count,
                                                    topic_coverage_count = topic_coverage_count,
                                                    topic_media_count = topic_media_count,
                                                    topic_leader_count = topic_leader_count
                            )
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')
    
@mod.route("/topic_ajax_path/", methods=['GET', 'POST'])
def topic_ajax_path():
    import urllib2
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == "GET":
                keyword = request.args.get('keyword', "")
                topic_id = request.args.get('topic_id', "")
                topics = db.session.query(Topic).filter(Topic.topicName==keyword).all()
                if len(topics):
                    for topic in topics:
                        keyid = topic.id
                else:        
                    new_item = Topic(topicName=keyword)
                    db.session.add(new_item)
                    db.session.commit()
                    topics = db.session.query(Topic).filter(Topic.topicName==keyword).all()
                    for topic in topics:
                        keyid = topic.id
                
                flag = forest_main(keyword,topic_id,keyid)
                return render_template('propagate/ajax/topic_retweetpath.html',keyid = keyid,flag = flag)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            keyword = request.args.get('keyword', "")
                            topic_id = request.args.get('topic_id', "")
                            topics = db.session.query(Topic).filter(Topic.topicName==keyword).all()
                            if len(topics):
                                for topic in topics:
                                    keyid = topic.id
                            else:        
                                new_item = Topic(topicName=keyword)
                                db.session.add(new_item)
                                db.session.commit()
                                topics = db.session.query(Topic).filter(Topic.topicName==keyword).all()
                                for topic in topics:
                                    keyid = topic.id
                
                            flag = forest_main(keyword,topic_id,keyid)
                            return render_template('propagate/ajax/topic_retweetpath.html',keyid = keyid,flag = flag)
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/topic_ajax_userfield/", methods=['GET', 'POST'])
def topic_ajax_userfield():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == "GET":
                keyword = request.args.get('topic_id', "")
                
                topic_info = readPropagateUser(keyword)
                topic_key_user_list = []

                domain = {'财经':0,'媒体':0,'文化':0,'科技':0,'娱乐':0,'教育':0,'时尚':0,'体育':0,'境外':0,'高校微博':0,'境内机构':0,'境外机构':0,'境内媒体':0,'境外媒体':0,'民间组织':0,'律师':0,'政府官员':0,'媒体人士':0,'活跃人士':0,'草根':0,'其他':0}
                for i in range(0,len(topic_info)):
                    topic_key_user_list.append({'id':topic_info[i]['id'],'name':topic_info[i]['name'],'statuses_count':topic_info[i]['status'],'followers_count':topic_info[i]['follower'],'bi_followers_count':topic_info[i]['friend'],'profile_image_url':topic_info[i]['image_url'],'description':topic_info[i]['description']})
                    area = user2domain(topic_info[i]['id'])
                    if area == -1:
                        area = 20
                    text = fieldsEn2Zh(fields_value[area])
                    domain[text] = domain[text] + 1                

                data2=[]
                if domain['高校微博'] >= 0:
                    data2.append({'university':domain['高校微博']})
                if domain['境内机构'] >= 0:
                    data2.append({'homeadmin':domain['境内机构']})
                if domain['境外机构'] >= 0:
                    data2.append({'abroadadmin':domain['境外机构']})
                if domain['境内媒体'] >= 0:
                    data2.append({'homemedia':domain['境内媒体']})
                if domain['境外媒体'] >= 0:
                    data2.append({'abroadmedia':domain['境外媒体']})
                if domain['民间组织'] >= 0:
                    data2.append({'folkorg':domain['民间组织']})
                if domain['律师'] >= 0:
                    data2.append({'lawyer':domain['律师']})
                if domain['政府官员'] >= 0:
                    data2.append({'politician':domain['政府官员']})
                if domain['媒体人士'] >= 0:
                    data2.append({'mediaworker':domain['媒体人士']})
                if domain['活跃人士'] >= 0:
                    data2.append({'activer':domain['活跃人士']})
                if domain['草根'] >= 0:
                    data2.append({'grassroot':domain['草根']})
                if domain['其他'] >= 0:
                    data2.append({'unknown':domain['其他']})

                topic_key_user_list = topic_key_user_list[:100]
                return render_template('propagate/ajax/topic_userfield.html',  topic_key_user_list= topic_key_user_list, topic_id=keyword, data2=data2)

            else:
                pass
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            keyword = request.args.get('topic_id', "")
                
                            topic_info = readPropagateUser(keyword)
                            topic_key_user_list = []

                            domain = {'财经':0,'媒体':0,'文化':0,'科技':0,'娱乐':0,'教育':0,'时尚':0,'体育':0,'境外':0,'高校微博':0,'境内机构':0,'境外机构':0,'境内媒体':0,'境外媒体':0,'民间组织':0,'律师':0,'政府官员':0,'媒体人士':0,'活跃人士':0,'草根':0,'其他':0}
                            for i in range(0,len(topic_info)):
                                topic_key_user_list.append({'id':topic_info[i]['id'],'name':topic_info[i]['name'],'statuses_count':topic_info[i]['status'],'followers_count':topic_info[i]['follower'],'bi_followers_count':topic_info[i]['friend'],'profile_image_url':topic_info[i]['image_url'],'description':topic_info[i]['description']})
                                area = user2domain(topic_info[i]['id'])
                                if area == -1:
                                    area = 20
                                text = fieldsEn2Zh(fields_value[area])
                                domain[text] = domain[text] + 1                

                            data2=[]
                            if domain['高校微博'] >= 0:
                                data2.append({'university':domain['高校微博']})
                            if domain['境内机构'] >= 0:
                                data2.append({'homeadmin':domain['境内机构']})
                            if domain['境外机构'] >= 0:
                                data2.append({'abroadadmin':domain['境外机构']})
                            if domain['境内媒体'] >= 0:
                                data2.append({'homemedia':domain['境内媒体']})
                            if domain['境外媒体'] >= 0:
                                data2.append({'abroadmedia':domain['境外媒体']})
                            if domain['民间组织'] >= 0:
                                data2.append({'folkorg':domain['民间组织']})
                            if domain['律师'] >= 0:
                                data2.append({'lawyer':domain['律师']})
                            if domain['政府官员'] >= 0:
                                data2.append({'politician':domain['政府官员']})
                            if domain['媒体人士'] >= 0:
                                data2.append({'mediaworker':domain['媒体人士']})
                            if domain['活跃人士'] >= 0:
                                data2.append({'activer':domain['活跃人士']})
                            if domain['草根'] >= 0:
                                data2.append({'grassroot':domain['草根']})
                            if domain['其他'] >= 0:
                                data2.append({'unknown':domain['其他']})

                            topic_key_user_list = topic_key_user_list[:100]
                            return render_template('propagate/ajax/topic_userfield.html',  topic_key_user_list= topic_key_user_list, topic_id=keyword, data2=data2)
                        else:
                            pass
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')
    
@mod.route("/showresult_single/", methods = ["GET","POST"])
def single_analysis():
    post_time = request.args.get('time', '')
    mid = request.args.get('mid', '')

    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin': 
                
            blog_info = readPropagateSingle(mid)#返回整个树的统计


            if blog_info:
                if blog_info[0]['profile_image_url'] == 'None':
                    blog_img_url = '#'
                else:
                    blog_img_url = blog_info[0]['profile_image_url']
                bloger_name = blog_info[0]['name']
                blog_reposts_count = blog_info[0]['repostsCount']
                blog_comments_count = blog_info[0]['commentsCount']
                blog_attitudes_count = blog_info[0]['attitudesCount']
                blog_time = blog_info[0]['postDate']
                blog_text = blog_info[0]['text']
            else:
                blog_info = readPropagateSinglePart(mid)
                if blog_info[0]['profile_image_url'] == 'None':
                    blog_img_url = '#'
                else:
                    blog_img_url = blog_info[0]['profile_image_url']
                bloger_name = blog_info[0]['name']
                blog_reposts_count = blog_info[0]['repostsCount']
                blog_comments_count = blog_info[0]['commentsCount']
                blog_attitudes_count = blog_info[0]['attitudesCount']
                blog_time = blog_info[0]['postDate']
                blog_text = blog_info[0]['text']

            return render_template('propagate/showResult_single.html', 
                                   mid=mid,
                                   tar_profile_image_url = blog_img_url,
                                   tar_screen_name = bloger_name,
                                   tar_repost_count = blog_reposts_count,
                                   tar_comments_count = blog_comments_count,
                                   tar_attitudes_count = blog_attitudes_count,
                                   tar_post_date = blog_time,
                                   post_time = post_time,
                                   tar_text = blog_text
                                   )
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        blog_info = readPropagateSingle(mid)#返回整个树的统计

                        if blog_info:          
                            blog_img_url = blog_info['profile_image_url']
                            bloger_name = blog_info[0]['name']
                            blog_reposts_count = blog_info[0]['repostsCount']
                            blog_comments_count = blog_info[0]['commentsCount']
                            blog_attitudes_count = blog_info[0]['attitudesCount']
                            blog_time = blog_info[0]['postDate']
                            blog_text = blog_info[0]['text']
                        else:
                            blog_info = readPropagateSinglePart(mid)
                            blog_img_url = blog_info[0]['profile_image_url']
                            bloger_name = blog_info[0]['name']
                            blog_reposts_count = blog_info[0]['repostsCount']
                            blog_comments_count = blog_info[0]['commentsCount']
                            blog_attitudes_count = blog_info[0]['attitudesCount']
                            blog_time = blog_info[0]['postDate']
                            blog_text = blog_info[0]['text']

                        return render_template('propagate/showResult_single.html', 
                                               mid=mid,
                                               tar_profile_image_url = blog_img_url,
                                               tar_screen_name = bloger_name,
                                               tar_repost_count = blog_reposts_count,
                                               tar_comments_count = blog_comments_count,
                                               tar_attitudes_count = blog_attitudes_count,
                                               tar_post_date = blog_time,
                                               post_time = post_time,
                                               tar_text = blog_text
                                               )
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/single_ajax_trend/", methods = ["GET","POST"])
def single_ajax_trend():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == "GET":
                mid = request.args.get('mid')
                return render_template('propagate/ajax/single_trend.html', mid=mid)
            else:
                mid = str(request.form.get('mid', ""))  
                retweeted_mid = getWeiboRetweetedStatus(mid)
                if retweeted_mid:
                    blog_info = readPropagateTrendSingle(retweeted_mid)
                else:
                    blog_info = readPropagateTrendSingle(mid)

                #print blog_info
                if blog_info:
                    perday_repost_count = blog_info['perday_count']
                    blog_date_list = blog_info['date_list']
                    date_list = [int(time.mktime(d.timetuple())+24*3600)*1000 for d in blog_date_list]
                else:
                    perday_repost_count = [1]
                    date_list = []

                blog_info_part = readPropagateTrendSinglePart(mid)
                if blog_info_part:
                    perday_repost_count_part = blog_info_part['perday_count']
                    blog_date_list_part = blog_info_part['date_list']
                    date_list_part = [int(time.mktime(d.timetuple())+24*3600)*1000 for d in blog_date_list_part]
                    date_list_part = []
                else:
                    perday_repost_count_part = [1]
                    date_list_part = []
                
                return json.dumps({'perday_blog_count': zip(date_list, perday_repost_count),'perday_blog_count_part': zip(date_list_part, perday_repost_count_part)})
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            return render_template('propagate/ajax/single_trend.html')
                        else:
                            mid = str(request.form.get('mid', ""))
                            post_time = request.form.get('post_time', "")
            
                            blog_info = readPropagateTrendSingle(mid)
                            if blog_info:
                                perday_repost_count = blog_info['perday_count']
                                blog_date_list = blog_info['date_list']
                                date_list = [int(time.mktime(d.timetuple())+24*3600)*1000 for d in blog_date_list]
                            else:
                                perday_repost_count = [1]                    
                                date_list = [int(datetime2ts(post_time)+24*3600)*1000]

                            blog_info_part = readPropagateTrendSinglePart(mid)
                            if blog_info_part:
                                perday_repost_count_part = blog_info_part['perday_count']
                                blog_date_list_part = blog_info_part['date_list']
                                date_list_part = [int(time.mktime(d.timetuple())+24*3600)*1000 for d in blog_date_list_part]
                            else:
                                perday_repost_count_part = [1]
                                date_list_part = [int(datetime2ts(post_time)+24*3600)*1000]
                
                            return json.dumps({'perday_blog_count': zip(date_list, perday_repost_count),'perday_blog_count_part': zip(date_list_part, perday_repost_count_part)})
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/single_ajax_weibos/")
def single_ajax_weibos():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == "GET":
                mid = str(request.args.get('mid', ""))
                retweeted_mid = getWeiboRetweetedStatus(mid)

                if retweeted_mid:
                    blog_info = readPropagateWeiboSingle(retweeted_mid)
                else:
                    blog_info = readPropagateWeiboSingle(mid)

                blog_info_part = readPropagateWeiboSinglePart(mid)

                return render_template('propagate/ajax/single_weibos.html', 
                                       blog_info = blog_info,
                                       blog_info_part = blog_info_part,
                                       mid = mid
                                      )
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            mid = str(request.args.get('mid', ""))

                            blog_info = readPropagateWeiboSingle(mid)
                            blog_info_part = readPropagateWeiboSinglePart(mid)
                            return render_template('propagate/ajax/single_weibos.html', 
                                                   blog_info = blog_info,
                                                   blog_info_part = blog_info_part,
                                                   mid = mid
                                                  )
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/single_ajax_spatial/", methods=['GET', 'POST'])
def single_ajax_spatial():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == "GET":
                mid = request.args.get('mid')
                return render_template('propagate/ajax/single_spatial.html', mid=mid)
            else:
                mid = str(request.form.get('mid', ""))
                retweeted_mid = getWeiboRetweetedStatus(mid)
                print retweeted_mid
                if retweeted_mid:
                    area_list = readPropagateSpatialSingle(retweeted_mid)
                else:
                    area_list = readPropagateSpatialSingle(mid)

                area_list_part = readPropagateSpatialSinglePart(mid)
                return json.dumps({'map_data': area_list,'map_data_part': area_list_part})
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            return render_template('propagate/ajax/single_spatial.html')
                        else:
                            mid = str(request.form.get('mid', ""))
                            area_list = readPropagateSpatialSingle(mid)
                            area_list_part = readPropagateSpatialSinglePart(mid)
                            return json.dumps({'map_data': area_list,'map_data_part': area_list_part})
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/single_ajax_stat/")
def single_ajax_stat():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == 'GET':
                mid = int(request.args.get('mid', ""))
                retweeted_mid = getWeiboRetweetedStatus(mid)
                if retweeted_mid:
                    blog_info = readIndexSingle(retweeted_mid)
                else:
                    blog_info = readIndexSingle(mid)
                if blog_info: 
                    tar_persistent_count = blog_info['persistent_index']
                    tar_sudden_count = blog_info['sudden_index']
                    tar_coverage_count = blog_info['coverage_index']
                    tar_media_count = blog_info['media_index']
                    tar_leader_count = blog_info['leader_index']
                else:
                    tar_persistent_count = 0
                    tar_sudden_count = 0
                    tar_coverage_count = 0
                    tar_media_count = 0
                    tar_leader_count = 0

                blog_info_part = readIndexSinglePart(mid)
                if blog_info_part:
                    tar_persistent_count_part = blog_info_part['persistent_index']
                    tar_sudden_count_part = blog_info_part['sudden_index']
                    tar_coverage_count_part = blog_info_part['coverage_index']
                    tar_media_count_part = blog_info_part['media_index']
                    tar_leader_count_part = blog_info_part['leader_index']
                else:
                    tar_persistent_count_part = 0
                    tar_sudden_count_part = 0
                    tar_coverage_count_part = 0
                    tar_media_count_part = 0
                    tar_leader_count_part = 0

                return render_template('propagate/ajax/single_stat.html',
                                        tar_persistent_count = tar_persistent_count,
                                        tar_sudden_count = tar_sudden_count,
                                        tar_coverage_count = tar_coverage_count,
                                        tar_media_count = tar_media_count,
                                        tar_leader_count = tar_leader_count,
                                        tar_persistent_count_part = tar_persistent_count_part,
                                        tar_sudden_count_part = tar_sudden_count_part,
                                        tar_coverage_count_part = tar_coverage_count_part,
                                        tar_media_count_part = tar_media_count_part,
                                        tar_leader_count_part = tar_leader_count_part
                )
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == 'GET':
                            mid = int(request.args.get('mid', ""))
                            retweeted_mid = getWeiboRetweetedStatus(mid)
                            if retweeted_mid:
                                blog_info = readIndexSingle(retweeted_mid)
                            else:
                                blog_info = readIndexSingle(mid)

                            if blog_info: 
                                tar_persistent_count = blog_info['persistent_index']
                                tar_sudden_count = blog_info['sudden_index']
                                tar_coverage_count = blog_info['coverage_index']
                                tar_media_count = blog_info['media_index']
                                tar_leader_count = blog_info['leader_index']
                            else:
                                tar_persistent_count = 0
                                tar_sudden_count = 0
                                tar_coverage_count = 0
                                tar_media_count = 0
                                tar_leader_count = 0

                            blog_info_part = readIndexSinglePart(mid)
                            if blog_info_part:
                                tar_persistent_count_part = blog_info_part['persistent_index']
                                tar_sudden_count_part = blog_info_part['sudden_index']
                                tar_coverage_count_part = blog_info_part['coverage_index']
                                tar_media_count_part = blog_info_part['media_index']
                                tar_leader_count_part = blog_info_part['leader_index']
                            else:
                                tar_persistent_count_part = 0
                                tar_sudden_count_part = 0
                                tar_coverage_count_part = 0
                                tar_media_count_part = 0
                                tar_leader_count_part = 0

                            return render_template('propagate/ajax/single_stat.html',
                                                    tar_persistent_count = tar_persistent_count,
                                                    tar_sudden_count = tar_sudden_count,
                                                    tar_coverage_count = tar_coverage_count,
                                                    tar_media_count = tar_media_count,
                                                    tar_leader_count = tar_leader_count,
                                                    tar_persistent_count_part = tar_persistent_count_part,
                                                    tar_sudden_count_part = tar_sudden_count_part,
                                                    tar_coverage_count_part = tar_coverage_count_part,
                                                    tar_media_count_part = tar_media_count_part,
                                                    tar_leader_count_part = tar_leader_count_part
                            )
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/single_ajax_path/", methods=['GET', 'POST'])
def single_ajax_path():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == "GET":
                mid = int(request.args.get('mid', ""))
                beg_ts = float(request.args.get('beg_ts', ""))
                end_ts = float(request.args.get('end_ts', ""))
                flag = tree_main(mid,beg_ts,end_ts)
                return render_template('propagate/ajax/single_retweetpath.html',mid = mid,flag = flag)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            mid = int(request.args.get('mid', ""))
                            flag = tree_main(mid)
                            return render_template('propagate/ajax/single_retweetpath.html',mid = mid,flag = flag)
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/single_ajax_userfield/", methods=['GET', 'POST'])
def single_ajax_userfield():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            if request.method == "GET":
                mid = str(request.args.get('mid', ""))
                retweeted_mid = getWeiboRetweetedStatus(mid)
                if retweeted_mid:
                    blog_key_user_list = readPropagateUserSingle(retweeted_mid)
                else:
                    blog_key_user_list = readPropagateUserSingle(mid)

                domain = {'财经':0,'媒体':0,'文化':0,'科技':0,'娱乐':0,'教育':0,'时尚':0,'体育':0,'境外':0,'高校微博':0,'境内机构':0,'境外机构':0,'境内媒体':0,'境外媒体':0,'民间组织':0,'律师':0,'政府官员':0,'媒体人士':0,'活跃人士':0,'草根':0,'其他':0}
                for result in blog_key_user_list:                    
                    area = user2domain(result['id'])
                    if area == -1:
                        area = 20
                    text = fieldsEn2Zh(fields_value[area])
                    domain[text] = domain[text] + 1

                data2=[]
                if domain['高校微博'] >= 0:
                    data2.append({'university':domain['高校微博']})
                if domain['境内机构'] >= 0:
                    data2.append({'homeadmin':domain['境内机构']})
                if domain['境外机构'] >= 0:
                    data2.append({'abroadadmin':domain['境外机构']})
                if domain['境内媒体'] >= 0:
                    data2.append({'homemedia':domain['境内媒体']})
                if domain['境外媒体'] >= 0:
                    data2.append({'abroadmedia':domain['境外媒体']})
                if domain['民间组织'] >= 0:
                    data2.append({'folkorg':domain['民间组织']})
                if domain['律师'] >= 0:
                    data2.append({'lawyer':domain['律师']})
                if domain['政府官员'] >= 0:
                    data2.append({'politician':domain['政府官员']})
                if domain['媒体人士'] >= 0:
                    data2.append({'mediaworker':domain['媒体人士']})
                if domain['活跃人士'] >= 0:
                    data2.append({'activer':domain['活跃人士']})
                if domain['草根'] >= 0:
                    data2.append({'grassroot':domain['草根']})
                if domain['其他'] >= 0:
                    data2.append({'unknown':domain['其他']})

                blog_key_user_list = blog_key_user_list[:100]

                blog_key_user_list_part = readPropagateUserSinglePart(mid)

                domain = {'财经':0,'媒体':0,'文化':0,'科技':0,'娱乐':0,'教育':0,'时尚':0,'体育':0,'境外':0,'高校微博':0,'境内机构':0,'境外机构':0,'境内媒体':0,'境外媒体':0,'民间组织':0,'律师':0,'政府官员':0,'媒体人士':0,'活跃人士':0,'草根':0,'其他':0}
                for result in blog_key_user_list_part:                    
                    area = user2domain(result['id'])
                    if area == -1:
                        area = 20
                    text = fieldsEn2Zh(fields_value[area])
                    domain[text] = domain[text] + 1

                data_part=[]
                if domain['高校微博'] >= 0:
                    data_part.append({'university':domain['高校微博']})
                if domain['境内机构'] >= 0:
                    data_part.append({'homeadmin':domain['境内机构']})
                if domain['境外机构'] >= 0:
                    data_part.append({'abroadadmin':domain['境外机构']})
                if domain['境内媒体'] >= 0:
                    data_part.append({'homemedia':domain['境内媒体']})
                if domain['境外媒体'] >= 0:
                    data_part.append({'abroadmedia':domain['境外媒体']})
                if domain['民间组织'] >= 0:
                    data_part.append({'folkorg':domain['民间组织']})
                if domain['律师'] >= 0:
                    data_part.append({'lawyer':domain['律师']})
                if domain['政府官员'] >= 0:
                    data_part.append({'politician':domain['政府官员']})
                if domain['媒体人士'] >= 0:
                    data_part.append({'mediaworker':domain['媒体人士']})
                if domain['活跃人士'] >= 0:
                    data_part.append({'activer':domain['活跃人士']})
                if domain['草根'] >= 0:
                    data_part.append({'grassroot':domain['草根']})
                if domain['其他'] >= 0:
                    data_part.append({'unknown':domain['其他']})

                blog_key_user_list_part = blog_key_user_list_part[:100]
                return render_template('propagate/ajax/single_userfield.html',  mid=mid, blog_key_user_list=blog_key_user_list, data2=data2, blog_key_user_list_part=blog_key_user_list_part, data_part=data_part)
            else:
                pass
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            mid = str(request.args.get('mid', ""))
                            retweeted_mid = getWeiboRetweetedStatus(mid)
                            if retweeted_mid:
                                blog_key_user_list = readPropagateUserSingle(retweeted_mid)
                            else:
                                blog_key_user_list = readPropagateUserSingle(mid)

                            domain = {'财经':0,'媒体':0,'文化':0,'科技':0,'娱乐':0,'教育':0,'时尚':0,'体育':0,'境外':0,'高校微博':0,'境内机构':0,'境外机构':0,'境内媒体':0,'境外媒体':0,'民间组织':0,'律师':0,'政府官员':0,'媒体人士':0,'活跃人士':0,'草根':0,'其他':0}
                            for result in blog_key_user_list:                    
                                area = user2domain(result['id'])
                                if area == -1:
                                    area = 20
                                text = fieldsEn2Zh(fields_value[area])
                                domain[text] = domain[text] + 1

                            data2=[]
                            if domain['高校微博'] >= 0:
                                data2.append({'university':domain['高校微博']})
                            if domain['境内机构'] >= 0:
                                data2.append({'homeadmin':domain['境内机构']})
                            if domain['境外机构'] >= 0:
                                data2.append({'abroadadmin':domain['境外机构']})
                            if domain['境内媒体'] >= 0:
                                data2.append({'homemedia':domain['境内媒体']})
                            if domain['境外媒体'] >= 0:
                                data2.append({'abroadmedia':domain['境外媒体']})
                            if domain['民间组织'] >= 0:
                                data2.append({'folkorg':domain['民间组织']})
                            if domain['律师'] >= 0:
                                data2.append({'lawyer':domain['律师']})
                            if domain['政府官员'] >= 0:
                                data2.append({'politician':domain['政府官员']})
                            if domain['媒体人士'] >= 0:
                                data2.append({'mediaworker':domain['媒体人士']})
                            if domain['活跃人士'] >= 0:
                                data2.append({'activer':domain['活跃人士']})
                            if domain['草根'] >= 0:
                                data2.append({'grassroot':domain['草根']})
                            if domain['其他'] >= 0:
                                data2.append({'unknown':domain['其他']})

                            blog_key_user_list = blog_key_user_list[:100]

                            blog_key_user_list_part = readPropagateUserSinglePart(mid)

                            domain = {'财经':0,'媒体':0,'文化':0,'科技':0,'娱乐':0,'教育':0,'时尚':0,'体育':0,'境外':0,'高校微博':0,'境内机构':0,'境外机构':0,'境内媒体':0,'境外媒体':0,'民间组织':0,'律师':0,'政府官员':0,'媒体人士':0,'活跃人士':0,'草根':0,'其他':0}
                            for result in blog_key_user_list_part:                    
                                area = user2domain(result['id'])
                                if area == -1:
                                    area = 20
                                text = fieldsEn2Zh(fields_value[area])
                                domain[text] = domain[text] + 1

                            data_part=[]
                            if domain['高校微博'] >= 0:
                                data_part.append({'university':domain['高校微博']})
                            if domain['境内机构'] >= 0:
                                data_part.append({'homeadmin':domain['境内机构']})
                            if domain['境外机构'] >= 0:
                                data_part.append({'abroadadmin':domain['境外机构']})
                            if domain['境内媒体'] >= 0:
                                data_part.append({'homemedia':domain['境内媒体']})
                            if domain['境外媒体'] >= 0:
                                data_part.append({'abroadmedia':domain['境外媒体']})
                            if domain['民间组织'] >= 0:
                                data_part.append({'folkorg':domain['民间组织']})
                            if domain['律师'] >= 0:
                                data_part.append({'lawyer':domain['律师']})
                            if domain['政府官员'] >= 0:
                                data_part.append({'politician':domain['政府官员']})
                            if domain['媒体人士'] >= 0:
                                data_part.append({'mediaworker':domain['媒体人士']})
                            if domain['活跃人士'] >= 0:
                                data_part.append({'activer':domain['活跃人士']})
                            if domain['草根'] >= 0:
                                data_part.append({'grassroot':domain['草根']})
                            if domain['其他'] >= 0:
                                data_part.append({'unknown':domain['其他']})

                            blog_key_user_list_part = blog_key_user_list_part[:100]
                            return render_template('propagate/ajax/single_userfield.html',  mid=mid, blog_key_user_list=blog_key_user_list, data2=data2, blog_key_user_list_part=blog_key_user_list_part, data_part=data_part)
                        else:
                            pass
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/add_material", methods = ["GET","POST"])
def add_material():
    result = 'Right'
    mid = int(request.form['mid'])
    ori_mid = str(request.form['ori_mid'])
    flag = int(request.form['flag'])

    ma_ids = db.session.query(M_Weibo).filter(M_Weibo.weibo_id==mid).all()
    if len(ma_ids):
        result = 'Wrong'
    else:
        mid = str(mid)
        blog = getMaterial(mid,ori_mid,flag)
        if blog:
            new_item = M_Weibo(weibo_id=blog['_id'],text=blog['text'],repostsCount=blog['reposts_count'],commentsCount=blog['comments_count'],postDate=blog['created_at'],uid=blog['id'])
            db.session.add(new_item)
            db.session.commit()
        else:
            result = 'Not Found'
    return json.dumps(result)

@mod.route("/add_material_topic", methods = ["GET","POST"])
def add_material_topic():
    result = 'Right'
    mid = int(request.form['mid'])
    topic_id = int(request.form['topic_id'])

    ma_ids = db.session.query(M_Weibo).filter(M_Weibo.weibo_id==mid).all()
    if len(ma_ids):
        result = 'Wrong'
    else:
        mid = str(mid)
        blog = getMaterialTopic(mid,topic_id)
        if blog:
            new_item = M_Weibo(weibo_id=blog['_id'],text=blog['text'],repostsCount=blog['reposts_count'],commentsCount=blog['comments_count'],postDate=blog['created_at'],uid=blog['id'])
            db.session.add(new_item)
            db.session.commit()
        else:
            result = 'Not Found'
    return json.dumps(result)

@mod.route('/single_rank/')
def single_rank():
    page = 1
    countperpage = 10
    limit = 1000000
    if request.args.get('page'):
        page = int(request.args.get('page'))
    if request.args.get('countperpage'):
        countperpage = int(request.args.get('countperpage'))
    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
    if request.args.get('mid'):
        mid = str(request.args.get('mid'))
    blog_key_user_list = readPropagateUserSingle(mid)

    if page == 1:
        startoffset = 0
    else:
        startoffset = (page - 1) * countperpage
    endoffset = startoffset + countperpage

    news=[]
    for i in range(0,len(blog_key_user_list)):
        if i>=startoffset and i<endoffset:
            status = user_status(blog_key_user_list[i]['id'])
            area = user2domain(blog_key_user_list[i]['id'])
            if area == -1:
                area = 20
            text = fieldsEn2Zh(fields_value[area])
            news.append({'id':blog_key_user_list[i]['id'],'name':blog_key_user_list[i]['name'],'location':blog_key_user_list[i]['location'],'followers_count':blog_key_user_list[i]['follower'],'bi_followers_count':blog_key_user_list[i]['friend'],'statuses_count':blog_key_user_list[i]['status'],'status':status,'domain':text})
    
    total_pages = limit / countperpage + 1
    return json.dumps({'news': news, 'pages': total_pages})

@mod.route('/single_rank_part/')
def single_rank_part():
    page = 1
    countperpage = 10
    limit = 1000000
    if request.args.get('page'):
        page = int(request.args.get('page'))
    if request.args.get('countperpage'):
        countperpage = int(request.args.get('countperpage'))
    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
    if request.args.get('mid'):
        mid = str(request.args.get('mid'))
    blog_key_user_list = readPropagateUserSinglePart(mid)

    if page == 1:
        startoffset = 0
    else:
        startoffset = (page - 1) * countperpage
    endoffset = startoffset + countperpage

    news=[]
    for i in range(0,len(blog_key_user_list)):
        if i>=startoffset and i<endoffset:
            status = user_status(blog_key_user_list[i]['id'])
            area = user2domain(blog_key_user_list[i]['id'])
            if area == -1:
                area = 20
            text = fieldsEn2Zh(fields_value[area])
            news.append({'id':blog_key_user_list[i]['id'],'name':blog_key_user_list[i]['name'],'location':blog_key_user_list[i]['location'],'followers_count':blog_key_user_list[i]['follower'],'bi_followers_count':blog_key_user_list[i]['friend'],'statuses_count':blog_key_user_list[i]['status'],'status':status,'domain':text})
    
    total_pages = limit / countperpage + 1
    return json.dumps({'news': news, 'pages': total_pages})

@mod.route('/topic_rank/')
def topic_rank():
    page = 1
    countperpage = 10
    limit = 1000000
    if request.args.get('page'):
        page = int(request.args.get('page'))
    if request.args.get('countperpage'):
        countperpage = int(request.args.get('countperpage'))
    if request.args.get('limit'):
        limit = int(request.args.get('limit'))

    keyword = request.args.get('topic_id', "")
    topic_info = readPropagateUser(keyword)    

    if page == 1:
        startoffset = 0
    else:
        startoffset = (page - 1) * countperpage
    endoffset = startoffset + countperpage

    news=[]
    for i in range(0,len(topic_info)):
        if i>=startoffset and i<endoffset:
            status = user_status(topic_info[i]['id'])
            area = user2domain(topic_info[i]['id'])
            if area == -1:
                area = 20
            text = fieldsEn2Zh(fields_value[area])
            news.append({'id':topic_info[i]['id'],'name':topic_info[i]['name'],'location':topic_info[i]['location'],'followers_count':topic_info[i]['follower'],'bi_followers_count':topic_info[i]['friend'],'statuses_count':topic_info[i]['status'],'status':status,'domain':text})
       
    total_pages = limit / countperpage + 1
    return json.dumps({'news': news, 'pages': total_pages})

@mod.route("/add_kd/", methods=['GET','POST'])
def add_kd():
    result = 'Right'
    new_field = request.form['f_id']
    count, get_results = xapian_search_user.search(query={'_id': new_field}, fields=['_id', 'name'])
    if count > 0:
        for get_result in get_results():
            new_item = KnowledgeList(kID=get_result['_id'],kName=get_result['name'])
            db.session.add(new_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route("/remove_kd/", methods=['GET','POST'])
def remove_kd():
    result = 'Right'
    new_id = request.form['f_id']
    old_items = db.session.query(KnowledgeList).filter(KnowledgeList.kID==new_id).all()
    if len(old_items):
        for old_item in old_items:
            db.session.delete(old_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route("/add_trash/", methods=['GET','POST'])
def add_trash():
    result = 'Right'
    new_field = request.form['f_id']
    count, get_results = xapian_search_user.search(query={'_id': new_field}, fields=['_id', 'name'])
    if count > 0:
        for get_result in get_results():
            new_item = BlackList(blackID=get_result['_id'],blackName=get_result['name'])
            db.session.add(new_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route('/history.json', methods=['GET','POST'])
def search_history():
    if request.method == 'GET':
        keyword = request.args.get('keyword',None)
        now1 = request.args.get('now1', None)
        now2 = request.args.get('now2', None)
        now = request.args.get('now', None)
        timestamp_end = request.args.get('timestamp', None)
        if timestamp_end:
            timestamp_end = int(timestamp_end)
        if now1:
            now1 = int(now1)
        if now2:
            now2 = int(now2)
        if now:
            now = int(now)
        histories1 = None
        histories2 = None
        histories = None
        if keyword != None:
            status, histories = _search_history(keyword)
        else:
            if now:
                status, histories = _all_history(now)
            if now1:
                status, histories1 = _all_history(now1)
            if now2 == 0:
                status, histories2 = _all_history(now2)
        histories_names = []
        if histories1:
            for history in histories1:
                start = time.strftime("%m月 %d日, %Y", time.localtime(history.start))
                end = time.strftime("%m月 %d日, %Y", time.localtime(history.end))
                datestr  = str(start) + ' - ' + str(end)
                if(timestamp_end):
                    timestamp_start = int(history.db_date)
                    time_pass = timestamp_end - timestamp_start
                    time_pass = time.strftime("%M分钟 %S秒 ", time.localtime(time_pass))
                    time_pass = '       已计算时长： ' + str(time_pass)
                    db_date = time.strftime("%m月 %d日, %Y %H:%M:%S", time.localtime(history.db_date))
                    db_date = '     提交时间： ' + str(db_date)
                    histories_names.append([history.topic, datestr, db_date, time_pass ])
                else:
                    histories_names.append([history.topic, datestr])
        if histories2:
            for history in histories2:
                start = time.strftime("%m月 %d日, %Y", time.localtime(history.start))
                end = time.strftime("%m月 %d日, %Y", time.localtime(history.end))
                datestr  = str(start) + ' - ' + str(end)
                if(timestamp_end):
                    timestamp_start = int(history.db_date)
                    time_pass = timestamp_end - timestamp_start
                    time_pass = time.strftime("%M分钟 %S秒 ", time.localtime(time_pass))
                    time_pass = '       已计算时长： ' + str(time_pass)
                    db_date = time.strftime("%m月 %d日, %Y %H:%M:%S", time.localtime(history.db_date))
                    db_date = '     提交时间： ' + str(db_date)
                    histories_names.append([history.topic, datestr, db_date, time_pass ])
                else:
                    histories_names.append([history.topic, datestr])                
        if histories:
            for history in histories:
                start = time.strftime("%m月 %d日, %Y", time.localtime(history.start))
                end = time.strftime("%m月 %d日, %Y", time.localtime(history.end))
                datestr  = str(start) + ' - ' + str(end)
                histories_names.append([history.topic, datestr])
        return json.dumps(histories_names)
    else:
        operator = request.form.get('operator', 'add')
        keyword = request.form.get('keyword', '')
        start = request.form.get('start', '')
        end = request.form.get('end', '')
        sentiment = request.form.get('sentiment', '')
        if keyword != '' and start != '' and end != '' and sentiment != '':
            if operator == 'add':
                status, item = _add_history(-1, keyword, start_ts, end_ts, timestamp)
                item = item.topic + '\t' + item.start + '\t' + item.end + '\t' + item.range + '\t' + item.status
            else:
                status, item = 'failed', 'Null'
        else:
            status, item = 'failed', 'Null'
        return json.dumps({'status': status, 'item': item})

@mod.route('/topic/submit', methods=['GET','POST'])
def topic_submit():

    keyword = request.args.get('keyword', None)
    time = request.args.get('time', None)
    timestamp = request.args.get('timestamp', None)
    timestamp = int(timestamp)
    time = _utf_encode(time)
    start_ts, end_ts = _time_zone(time)
    start_ts = date2ts(start_ts)
    end_ts = date2ts(end_ts)
    status , item = _add_history(-1, keyword, start_ts, end_ts, timestamp)
    return 'success'

@mod.route('/history_id.json', methods=['GET','POST'])
def search_history_id():
    if request.method == 'GET':
        mid = request.args.get('mid',None)
        now1 = request.args.get('now1', None)
        now2 = request.args.get('now2', None)
        now = request.args.get('now', None)
        timestamp_end = request.args.get('timestamp', None)
        if timestamp_end:
            timestamp_end = int(timestamp_end)
        if now1:
            now1 = int(now1)
        if now2:
            now2 = int(now2)
        if now:
            now = int(now)
        histories1 = None
        histories2 = None
        histories = None
        if mid != None:
            mid = str(mid)
            status, histories = _search_history_weibo(mid)
        else:
            if now:
                status, histories = _all_history_weibo(now)
            if now1:
                status, histories1 = _all_history_weibo(now1)
            if now2 == 0:
                status, histories2 = _all_history_weibo(now2)
        histories_names = []
        if histories1:
            for history in histories1:
                datestr  = str(history.postDate)
                if(timestamp_end):
                    timestamp_start = int(history.db_date)
                    time_pass = timestamp_end - timestamp_start
                    time_pass = time.strftime("%M分钟 %S秒 ", time.localtime(time_pass))
                    time_pass = '       已计算时长： ' + str(time_pass)
                    db_date = time.strftime("%m月 %d日, %Y %H:%M:%S", time.localtime(history.db_date))
                    db_date = '     提交时间： ' + str(db_date)
                    histories_names.append([history.mid, datestr, db_date, time_pass ])
                else:
                    histories_names.append([history.mid, datestr])
        if histories2:
            for history in histories2:
                datestr  = str(history.postDate)
                if(timestamp_end):
                    timestamp_start = int(history.db_date)
                    time_pass = timestamp_end - timestamp_start
                    time_pass = time.strftime("%M分钟 %S秒 ", time.localtime(time_pass))
                    time_pass = '       已计算时长： ' + str(time_pass)
                    db_date = time.strftime("%m月 %d日, %Y %H:%M:%S", time.localtime(history.db_date))
                    db_date = '     提交时间： ' + str(db_date)
                    histories_names.append([history.mid, datestr, db_date, time_pass ])
                else:
                    histories_names.append([history.mid, datestr])                
        if histories:
            for history in histories:
                datestr  = str(history.postDate)
                histories_names.append([history.mid, datestr])
        return json.dumps(histories_names)
    else:
        operator = request.form.get('operator', 'add')
        mid = request.form.get('mid', '')
        postDate = request.form.get('postDate', '')
        sentiment = request.form.get('sentiment', '')
        if keyword != '' and postDate != '' and sentiment != '':
            if operator == 'add':
                status, item = _add_history_weibo(-1, mid, postDate, timestamp)
                item = item.mid + '\t' + item.postDate + '\t' + item.status
            else:
                status, item = 'failed', 'Null'
        else:
            status, item = 'failed', 'Null'
        return json.dumps({'status': status, 'item': item})

@mod.route('/weibo/submit', methods=['GET','POST'])
def weibo_submit():
    mid = request.args.get('mid', None)
    time = request.args.get('time', None)
    timestamp = request.args.get('timestamp', None)
    timestamp = int(timestamp)
    time = _utf_encode(time)
    start_ts, end_ts = _time_zone(time)
    start_ts = date2ts(start_ts)
    end_ts = date2ts(end_ts)
    statuses_search = getXapianweiboByTs(start_ts, end_ts)
    count,get_results = statuses_search.search(query={'_id': mid},fields=['timestamp'])
    for r in get_results():
        postDate = datetime.fromtimestamp(r['timestamp'])
    status , item = _add_history_weibo(-1, mid, postDate, timestamp)
    return 'success'

@mod.route('/url2mid', methods=['GET','POST'])
def url_mid():
    url = request.args.get('url', None)
    if url:
        mid = get_mid(url)
    else:
        return 'Wrong'
    return str(mid)


def target_whole_xapian_weibo(start_date=XAPIAN_FIRST_DATE, end_date=XAPIAN_LAST_DATE):
    datelist = []
    start_ts = int(time.mktime(time.strptime(start_date, '%Y%m%d')))
    end_ts = int(time.mktime(time.strptime(end_date, '%Y%m%d'))) + 24 * 3600

    during = (end_ts - start_ts) / (24 * 3600)
    for i in range(0, during):
        now_date = time.strftime('%Y%m%d', time.localtime(start_ts + i * 24 * 3600))
        datelist.append(now_date)

    xapian_weibo = getXapianWeiboByDuration(datelist)

    return xapian_weibo

whole_xapian_weibo = target_whole_xapian_weibo('20130901', '20130905')


def getWeiboRetweetedStatus(mid):
    weibo = whole_xapian_weibo.search_by_id(int(mid), fields=['retweeted_mid'])
    retweeted_mid = None
    if weibo:
        retweeted_mid = weibo['retweeted_mid']

    return retweeted_mid

