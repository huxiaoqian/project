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

sys.path.append('./weibo/propagate/graph')
from tree import *
from forest import *

sys.path.append('./weibo/profile')
from utils import *

path = '/home/mirage/dev/data/stub/master_timeline_weibo_'
fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology']
fields_id = {'culture':1, 'education':2, 'entertainment':3, 'fashion':4, 'finance':5, 'media':6, 'sports':7, 'technology':8}
month_value = {'January':1, 'February':2, 'March':3, 'April':4, 'May':5, 'June':6, 'July':7, 'August':8, 'September':9, 'October':10, 'November':11, 'December':12}

mod = Blueprint('propagate', __name__, url_prefix='/propagate')

def ts2datetimestr(ts):
    return time.strftime('%Y%m%d', time.localtime(ts))

def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d %H:%M:%S'))

def date2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

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
##        ts = datetime(year, month, day, 0, 0, 0)
##        ts = time.mktime(ts.timetuple())
        ts = str(year)+'-'+str(month)+'-'+str(day)
        tslist.append(ts)

    start_ts = tslist[0]
    end_ts = tslist[1]

    return start_ts, end_ts

def getFieldTopics():
    topic_names = []
    topics = db.session.query(Topic)
    for topic in topics:
        topic_names.append(topic.topicName)
##        field_name = field.fieldName
##        topics = db.session.query(Topic).filter_by(field=field)
##        topic_names = []
##        for topic in topics:
##            topic_names.append(topic.topicName)
##        field_topics.append({'field_name': field_name, 'topics': topic_names, 'len': len(topic_names)})
    return topic_names

def getXapianWeiboByDuration(datestr_list):
    stub_file_list = []

    for datestr in datestr_list:
        stub_file = path + datestr
        print type(stub_file)
        if os.path.exists(stub_file):
            stub_file_list.append(stub_file)

    if len(stub_file_list):
        xapian_search_weibo = XapianSearch(stub=stub_file_list, include_remote=True)
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

def getHotStatus(start,end):

    statuses_search = getXapianweiboByTs(start,end)
    count,statuses = statuses_search.search(sort_by=['reposts_count'], fields=['_id','text','timestamp','user','reposts_count','comments_count','attitudes_count','retweeted_mid','source'], max_offset=10)
    status_hot = []             
    for status in statuses():
        uid = status['user']
        user = get_user(uid)
        weibo_url = weiboinfo2url(uid,status['_id'])
        status['text'] = status['text']
        status['source'] = status['source']
        status['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(status['timestamp']))
        status_hot.append({'status': status, 'user': user, 'weibo_url': weibo_url})
    return status_hot

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
            topics = getFieldTopics() 
            
            return render_template('propagate/search.html', topics=topics)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        topics = getFieldTopics()
     
                        return render_template('propagate/search.html', topics=topics)
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
            keyuser = request.args.get('keyuser', '')
            dur_time = request.args.get('time', '')

##            times = dur_time.split(' - ')
##            n = 0
##            for ti in times:
##                if n==0:
##                    beg_time = strToDate(ti)
##                    n = 1
##                else:
##                    end_time = strToDate(ti)

            dur_time = _utf_encode(dur_time)
            if not dur_time or dur_time == '':
                beg_time, end_time = _default_time_zone()
            else:
                dur_time = _utf_encode(dur_time)
                beg_time, end_time = _time_zone(dur_time)
            
            keyword = keyword.strip('@\r\n\t')
            keyuser = keyuser.strip('@\r\n\t')
            beg_time = beg_time.strip('@\r\n\t')
            end_time = end_time.strip('@\r\n\t')

            return_beg_str = beg_time
            return_end_str = end_time
            keyuser_str = keyuser

            if keyword == "":
                flash(u'关键字（词）不能为空')
                return redirect('/propagate/')
            if keyuser == "":
                keyuser_str = u'无'
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

            statuses_search = getXapianweiboByTs(beg_time, end_time)
            fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
            count, get_results = statuses_search.search(query={'text': [u'%s'%keyword]}, sort_by=['reposts_count'], fields=fields_list,max_offset=1000)
            if count == 0:
                flash(u'您搜索的话题结果为空')
                return redirect('/propagate/')
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
                                        topic_profile_image_url = '',# topic_img_url[0],
                                        topic_ori_screen_name = topic_ori_screen_name,
                                        blog_rel_count = topic_blog_count,
                                        blog_ori_count = topic_blog_ori_count,
                                        blog_ori_account = blog_ori_account,
                                        topic_leader_count = topic_leader_count,
                                        topic_ori_date = topic_ori_date,
                                        keyword=keyword, keyuser=keyuser, beg_time=beg_time, end_time=end_time,
                                        return_beg_str=return_beg_str, return_end_str=return_end_str, keyuser_str=keyuser_str
                )
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        keyword = request.args.get('keyword', '')
                        keyuser = request.args.get('keyuser', '')
                        dur_time = request.args.get('time', '')

                        dur_time = _utf_encode(dur_time)
                        if not dur_time or dur_time == '':
                            beg_time, end_time = _default_time_zone()
                        else:
                            dur_time = _utf_encode(dur_time)
                            beg_time, end_time = _time_zone(dur_time)
    
                        keyword = keyword.strip('@\r\n\t')
                        keyuser = keyuser.strip('@\r\n\t')
                        beg_time = beg_time.strip('@\r\n\t')
                        end_time = end_time.strip('@\r\n\t')

                        return_beg_str = beg_time
                        return_end_str = end_time
                        keyuser_str = keyuser
    
                        if keyword == "":
                            flash(u'关键字（词）不能为空')
                            return redirect('/propagate/')
                        if keyuser == "":
                            keyuser_str = u'无'
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
        
                        fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
                        count, get_results = xapian_search_weibo.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['timestamp'], fields=fields_list,max_offset=1000)

                        if count == 0:
                            flash(u'您搜索的话题结果为空')
                            return redirect('/propagate/')
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
                keyword = request.form.get('keyword', "")
                keyuser = request.form.get('keyuser', "")
                beg_time = int(request.form.get('beg_time', ""))
                end_time = int(request.form.get('end_time', ""))
                statuses_search = getXapianweiboByTs(beg_time, end_time)
                fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
                count, get_results = statuses_search.search(query={'text': [u'%s'%keyword]}, sort_by=['reposts_count'], fields=fields_list,max_offset=1000)

                topic_info = calculate(get_results())
                perday_blog_count = topic_info['perday_count_list']
                date_list = topic_info['date_list']
                date_list = [int(time.mktime(d.timetuple()))*1000 for d in date_list]
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
                            keyword = request.form.get('keyword', "")
                            keyuser = request.form.get('keyuser', "")
                            beg_time = int(request.form.get('beg_time', ""))
                            end_time = int(request.form.get('end_time', ""))
    
                            fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
                            count, get_results = xapian_search_weibo.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['timestamp'], fields=fields_list,max_offset=1000)

                            topic_info = calculate(get_results())
                            perday_blog_count = topic_info['perday_count_list']
                            date_list = topic_info['date_list']
                            date_list = [int(time.mktime(d.timetuple()))*1000 for d in date_list]

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
                keyword = request.args.get('keyword', "")
                keyuser = request.args.get('keyuser', "")
                beg_time = int(request.args.get('beg_time', ""))
                end_time = int(request.args.get('end_time', ""))
                statuses_search = getXapianweiboByTs(beg_time, end_time)
                fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
                count, get_results = statuses_search.search(query={'text': [u'%s'%keyword]}, sort_by=['reposts_count'], fields=fields_list,max_offset=100)

                topic_info = calculate(get_results())
                blog_rel_list = topic_info['topic_rel_blog'][:5]

                return render_template('propagate/ajax/topic_weibos.html', blog_rel_list= blog_rel_list)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            keyword = request.args.get('keyword', "")
                            keyuser = request.args.get('keyuser', "")
                            beg_time = int(request.args.get('beg_time', ""))
                            end_time = int(request.args.get('end_time', ""))
    
                            fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
                            count, get_results = xapian_search_weibo.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['reposts_count'], fields=fields_list,max_offset=100)

                            topic_info = calculate(get_results())
                            blog_rel_list = topic_info['topic_rel_blog'][:5]

                            return render_template('propagate/ajax/topic_weibos.html', blog_rel_list= blog_rel_list)
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
                keyword = request.form.get('keyword', "")
                keyuser = request.form.get('keyuser', "")
                beg_time = int(request.form.get('beg_time', ""))
                end_time = int(request.form.get('end_time', ""))
                statuses_search = getXapianweiboByTs(beg_time, end_time)
                fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
                count, get_results = statuses_search.search(query={'text': [u'%s'%keyword]}, sort_by=['reposts_count'], fields=fields_list,max_offset=1000)

                topic_info = calculate(get_results())
                topic_area_list = topic_info['geo']

                return json.dumps({'map_data': topic_area_list})
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            return render_template('propagate/ajax/topic_spatial.html')
                        else:
                            keyword = request.form.get('keyword', "")
                            keyuser = request.form.get('keyuser', "")
                            beg_time = int(request.form.get('beg_time', ""))
                            end_time = int(request.form.get('end_time', ""))
    
                            fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
                            count, get_results = xapian_search_weibo.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['timestamp'], fields=fields_list,max_offset=1000)

                            topic_info = calculate(get_results())
                            topic_area_list = topic_info['geo']

                            return json.dumps({'map_data': topic_area_list})
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
                keyword = request.args.get('keyword', "")
                keyuser = request.args.get('keyuser', "")
                beg_time = int(request.args.get('beg_time', ""))
                end_time = int(request.args.get('end_time', ""))
                statuses_search = getXapianweiboByTs(beg_time, end_time)
                fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
                count, get_results = statuses_search.search(query={'text': [u'%s'%keyword]}, sort_by=['reposts_count'], fields=fields_list,max_offset=1000)

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
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == 'GET':
                            keyword = request.args.get('keyword', "")
                            keyuser = request.args.get('keyuser', "")
                            beg_time = int(request.args.get('beg_time', ""))
                            end_time = int(request.args.get('end_time', ""))
    
                            fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
                            count, get_results = xapian_search_weibo.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['timestamp'], fields=fields_list,max_offset=1000)

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
                keyuser = request.args.get('keyuser', "")
                beg_time = int(request.args.get('beg_time', ""))
                end_time = int(request.args.get('end_time', ""))
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
                
                flag = forest_main(keyword,beg_time,end_time,keyid)
                return render_template('propagate/ajax/topic_retweetpath.html',keyid = keyid,flag = flag)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            keyword = request.args.get('keyword', "")
                            keyuser = request.args.get('keyuser', "")
                            beg_time = int(request.args.get('beg_time', ""))
                            end_time = int(request.args.get('end_time', ""))
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
                            flag = forest_main(keyword,beg_time,end_time,keyid)
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
                keyword = request.args.get('keyword', "")
                keyuser = request.args.get('keyuser', "")
                beg_time = int(request.args.get('beg_time', ""))
                end_time = int(request.args.get('end_time', ""))
                statuses_search = getXapianweiboByTs(beg_time, end_time)
                fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
                count, get_results = statuses_search.search(query={'text': [u'%s'%keyword]}, sort_by=['reposts_count'], fields=fields_list,max_offset=1000)

                topic_key_user_list = []
                domain = {'财经':0,'媒体':0,'文化':0,'科技':0,'娱乐':0,'教育':0,'时尚':0,'体育':0,'其他':0}
                for result in get_results():
                    number, users = xapian_search_user.search(query={'_id': result['user']}, fields=['_id','name','location','friends_count','followers_count'])
                    if not number:
                        continue
                    for user in users():

                        topic_key_user_list.append({'id':user['_id'],'name':user['name'],'location':user['location'],'followers_count':user['followers_count'],'bi_followers_count':user['friends_count']})
                    n, domains = xapian_search_domain.search(query={'_id': result['user']}, fields=['domain'])
                    if not n:
                        domain['其他'] = domain['其他'] + 1
                        continue
                    for do in domains():
                        if int(do['domain']) <= 7:
                            text = fieldsEn2Zh(fields_value[int(do['domain'])])
                            domain[text] = domain[text] + 1
                        else:
                            domain['其他'] = domain['其他'] + 1
                data=[]
                
                if domain['财经'] >= 0:
                    data.append({'finance':domain['财经']})
                if domain['媒体'] >= 0:
                    data.append({'media_domain':domain['媒体']})
                if domain['文化'] >= 0:
                    data.append({'culture':domain['文化']})
                if domain['科技'] >= 0:
                    data.append({'technology':domain['科技']})
                if domain['娱乐'] >= 0:
                    data.append({'entertainment':domain['娱乐']})
                if domain['教育'] >= 0:
                    data.append({'education':domain['教育']})
                if domain['时尚'] >= 0:
                    data.append({'fashion':domain['时尚']})
                if domain['体育'] >= 0:
                    data.append({'sports':domain['体育']})
                if domain['其他'] >= 0:
                    data.append({'unknown':domain['其他']})
                    
                return render_template('propagate/ajax/topic_userfield.html',  topic_key_user_list= topic_key_user_list, keyword=keyword, keyuser=keyuser, beg_time=beg_time, end_time=end_time, data=data)
            else:
                pass
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            keyword = request.args.get('keyword', "")
                            keyuser = request.args.get('keyuser', "")
                            beg_time = int(request.args.get('beg_time', ""))
                            end_time = int(request.args.get('end_time', ""))
    
                            fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
                            count, get_results = xapian_search_weibo.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['timestamp'], fields=fields_list,max_offset=1000)

                            topic_key_user_list = []
                            for result in get_results():
                                number, users = xapian_search_user.search(query={'_id': result['user']}, fields=['_id','name','location','friends_count','followers_count'])
                                if not number:
                                    continue
                                for user in users():

                                    topic_key_user_list.append({'id':user['_id'],'name':user['name'],'location':user['location'],'followers_count':user['followers_count'],'bi_followers_count':user['friends_count']})
                                    n, domains = xapian_search_domain.search(query={'_id': result['user']}, fields=['domain'])
                                    if not n:
                                        domain['其他'] = domain['其他'] + 1
                                        continue
                                    for do in domains():
                                        if int(do['domain']) <= 7:
                                            text = fieldsEn2Zh(fields_value[int(do['domain'])])
                                            domain[text] = domain[text] + 1
                                        else:
                                            domain['其他'] = domain['其他'] + 1
                            data=[]

                            if domain['财经'] >= 0:
                                data.append({'finance':domain['财经']})
                            if domain['媒体'] >= 0:
                                data.append({'media_domain':domain['媒体']})
                            if domain['文化'] >= 0:
                                data.append({'culture':domain['文化']})
                            if domain['科技'] >= 0:
                                data.append({'technology':domain['科技']})
                            if domain['娱乐'] >= 0:
                                data.append({'entertainment':domain['娱乐']})
                            if domain['教育'] >= 0:
                                data.append({'education':domain['教育']})
                            if domain['时尚'] >= 0:
                                data.append({'fashion':domain['时尚']})
                            if domain['体育'] >= 0:
                                data.append({'sports':domain['体育']})
                            if domain['其他'] >= 0:
                                data.append({'unknown':domain['其他']})
                            return render_template('propagate/ajax/topic_userfield.html',  topic_key_user_list= topic_key_user_list, keyword=keyword, keyuser=keyuser, beg_time=beg_time, end_time=end_time, data=data)
                        else:
                            pass
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/showresult_single/<mid>/<timestr>", methods = ["GET","POST"])
def single_analysis(mid,timestr):
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            mid = int(mid)
            timestr = str(timestr)
            time_ts = datetime2ts(timestr)
            time_date = ts2datetime(time_ts)
            time_ts = date2ts(time_date)
            blog_info = calculate_single(mid,time_ts)
                                 
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
                                   time_ts=time_ts
                                   )
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
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
                return render_template('propagate/ajax/single_trend.html')
            else:
                mid = int(request.form.get('mid', ""))
                time_ts = int(request.form.get('time_ts', ""))
                blog_info = calculate_single(mid,time_ts)
                perday_repost_count = blog_info['perday_count']
                blog_date_list = blog_info['datelist']
                date_list = [int(time.mktime(d.timetuple()))*1000 for d in blog_date_list]

                return json.dumps({'perday_blog_count': zip(date_list, perday_repost_count)})
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            return render_template('propagate/ajax/single_trend.html')
                        else:
                            mid = int(request.form.get('mid', ""))
                            blog_info = calculate_single(mid)
                            perday_repost_count = blog_info['perday_count']
                            blog_date_list = blog_info['datelist']
                            date_list = [int(time.mktime(d.timetuple()))*1000 for d in blog_date_list]

                            return json.dumps({'perday_blog_count': zip(date_list, perday_repost_count)})
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
                mid = int(request.args.get('mid', ""))
                time_ts = int(request.args.get('time_ts', ""))
                blog_info = calculate_single(mid,time_ts)

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
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
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
                return render_template('propagate/ajax/single_spatial.html')
            else:
                mid = int(request.form.get('mid', ""))
                time_ts = int(request.form.get('time_ts', ""))
                blog_info = calculate_single(mid,time_ts)
                area_list = blog_info['geo']

                return json.dumps({'map_data': area_list})
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            return render_template('propagate/ajax/single_spatial.html')
                        else:
                            mid = int(request.form.get('mid', ""))
                            blog_info = calculate_single(mid)
                            area_list = blog_info['geo']

                            return json.dumps({'map_data': area_list})
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
                time_ts = int(request.args.get('time_ts', ""))
                blog_info = calculate_single(mid,time_ts)

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
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
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
                time_ts = int(request.args.get('time_ts', ""))
                flag = tree_main(mid,time_ts)
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
                mid = int(request.args.get('mid', ""))
                time_ts = int(request.args.get('time_ts', ""))
                blog_info = calculate_single(mid,time_ts)

                repost_bloger = blog_info['repost_users']
                blog_key_user_list = repost_bloger

                domain = {'财经':0,'媒体':0,'文化':0,'科技':0,'娱乐':0,'教育':0,'时尚':0,'体育':0,'其他':0}
                for result in blog_key_user_list:                    
                    n, domains = xapian_search_domain.search(query={'_id': result['id']}, fields=['domain'])
                    if not n:
                        domain['其他'] = domain['其他'] + 1
                        continue
                    for do in domains():
                        if int(do['domain']) <= 7:
                            text = fieldsEn2Zh(fields_value[int(do['domain'])])
                            domain[text] = domain[text] + 1
                        else:
                            domain['其他'] = domain['其他'] + 1

                data=[]

                if domain['财经'] >= 0:
                    data.append({'finance':domain['财经']})
                if domain['媒体'] >= 0:
                    data.append({'media_domain':domain['媒体']})
                if domain['文化'] >= 0:
                    data.append({'culture':domain['文化']})
                if domain['科技'] >= 0:
                    data.append({'technology':domain['科技']})
                if domain['娱乐'] >= 0:
                    data.append({'entertainment':domain['娱乐']})
                if domain['教育'] >= 0:
                    data.append({'education':domain['教育']})
                if domain['时尚'] >= 0:
                    data.append({'fashion':domain['时尚']})
                if domain['体育'] >= 0:
                    data.append({'sports':domain['体育']})
                if domain['其他'] >= 0:
                    data.append({'unknown':domain['其他']})
                
                return render_template('propagate/ajax/single_userfield.html',  mid=mid, blog_key_user_list=blog_key_user_list, data=data)
            else:
                pass
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        if request.method == "GET":
                            mid = int(request.args.get('mid', ""))
                            blog_info = calculate_single(mid)

                            repost_bloger = blog_info['repost_users']
                            blog_key_user_list = repost_bloger

                            domain = {'财经':0,'媒体':0,'文化':0,'科技':0,'娱乐':0,'教育':0,'时尚':0,'体育':0,'其他':0}
                            for result in blog_key_user_list:                    
                                n, domains = xapian_search_domain.search(query={'_id': result['id']}, fields=['domain'])
                                if not n:
                                    domain['其他'] = domain['其他'] + 1
                                    continue
                                for do in domains():
                                    if int(do['domain']) <= 7:
                                        text = fieldsEn2Zh(fields_value[int(do['domain'])])
                                        domain[text] = domain[text] + 1
                                    else:
                                        domain['其他'] = domain['其他'] + 1
                            data=[]

                            if domain['财经'] >= 0:
                                data.append({'finance':domain['财经']})
                            if domain['媒体'] >= 0:
                                data.append({'media_domain':domain['媒体']})
                            if domain['文化'] >= 0:
                                data.append({'culture':domain['文化']})
                            if domain['科技'] >= 0:
                                data.append({'technology':domain['科技']})
                            if domain['娱乐'] >= 0:
                                data.append({'entertainment':domain['娱乐']})
                            if domain['教育'] >= 0:
                                data.append({'education':domain['教育']})
                            if domain['时尚'] >= 0:
                                data.append({'fashion':domain['时尚']})
                            if domain['体育'] >= 0:
                                data.append({'sports':domain['体育']})
                            if domain['其他'] >= 0:
                                data.append({'unknown':domain['其他']})

                            return render_template('propagate/ajax/single_userfield.html',  mid=mid, blog_key_user_list=blog_key_user_list, data=data)
                        else:
                            pass
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/add_material", methods = ["GET","POST"])
def add_material():
    print 'yuan'
    result = 'Right'
    mid = request.form['mid']
    mid = int(mid)
    blog_info = calculate_single(mid)
                                 
    blog_reposts_count = blog_info['status']['repostsCount']
    blog_comments_count = blog_info['status']['commentsCount']
    blog_time = blog_info['status']['postDate']
    blog_text = blog_info['status']['text']
    blog_id = blog_info['status']['id']
    ma_ids = db.session.query(M_Weibo).filter(M_Weibo.weibo_id==blog_id).all()
    if len(ma_ids):
        result = 'Wrong'
    else:
        new_item = M_Weibo(weibo_id=blog_id,text=blog_text,repostsCount=blog_reposts_count,commentsCount=blog_comments_count,postDate=blog_time,uid=blog_info['user']['id'])
        db.session.add(new_item)
        db.session.commit()
    return json.dumps(result)

@mod.route("/topics")
def topics():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            field_topics = []
            fields = db.session.query(Field)
            for field in fields:
                field_name = field.fieldName
                topics = db.session.query(Topic).filter_by(field=field)
                topic_names = []
                for topic in topics:
                    topic_names.append(topic.topicName)
                field_topics.append({'field_name': field_name, 'topics': topic_names, 'len': len(topic_names)})

            field_topics.sort(key=lambda x:x['len'],reverse=True)
            return render_template('propagate/topics.html',field_topics = field_topics)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        field_topics = []
                        fields = db.session.query(Field)
                        for field in fields:
                            field_name = field.fieldName
                            topics = db.session.query(Topic).filter_by(field=field)
                            topic_names = []
                            for topic in topics:
                                topic_names.append(topic.topicName)
                            field_topics.append({'field_name': field_name, 'topics': topic_names, 'len': len(topic_names)})
                        field_topics.sort(key=lambda x:x['len'],reverse=True)
                        return render_template('propagate/topics.html',field_topics = field_topics)
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/hot_status/")
def hot_status():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            dur_time = request.args.get('time', '')

            dur_time = _utf_encode(dur_time)
            if not dur_time or dur_time == '':
                beg_time, end_time = _default_time_zone()
            else:
                dur_time = _utf_encode(dur_time)
                beg_time, end_time = _time_zone(dur_time)
            
            status_hot = getHotStatus(beg_time,end_time)
            return render_template('propagate/hot_status.html',status_hot = status_hot)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.propagate
                    if identy == 1:
                        dur_time = request.args.get('time', '')

                        dur_time = _utf_encode(dur_time)
                        if not dur_time or dur_time == '':
                            beg_time, end_time = _default_time_zone()
                        else:
                            dur_time = _utf_encode(dur_time)
                            beg_time, end_time = _time_zone(dur_time)
            
                        status_hot = getHotStatus(beg_time,end_time)
                        return render_template('propagate/hot_status.html',status_hot = status_hot)
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

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
        mid = int(request.args.get('mid'))
    blog_info = calculate_single(mid)
    repost_bloger = blog_info['repost_users']
    blog_key_user_list = repost_bloger

    if page == 1:
        startoffset = 0
    else:
        startoffset = (page - 1) * countperpage
    endoffset = startoffset + countperpage

    news=[]
    n = 0
    for user in blog_key_user_list:
        if user:
            n = n + 1
            if n > startoffset:
                if n > endoffset:
                    break
                news.append({'id':user['id'],'name':user['name'],'location':user['location'],'followers_count':user['followers_count'],'bi_followers_count':user['bi_followers_count']})
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

    keyword = request.args.get('keyword', "")
    keyuser = request.args.get('keyuser', "")
    beg_time = int(request.args.get('beg_time', ""))
    end_time = int(request.args.get('end_time', ""))
    statuses_search = getXapianweiboByTs(beg_time, end_time)
    fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_mid','bmiddle_pic','geo','source','attitudes_count'] 
    count, get_results = statuses_search.search(query={'text': [u'%s'%keyword]}, sort_by=['reposts_count'], fields=fields_list,max_offset=1000)

    topic_key_user_list = []
    for result in get_results():
        number, users = xapian_search_user.search(query={'_id': result['user']}, fields=['_id','name','location','friends_count','followers_count'])
        if not number:
            continue
        for user in users():

            topic_key_user_list.append({'id':user['_id'],'name':user['name'],'location':user['location'],'followers_count':user['followers_count'],'bi_followers_count':user['friends_count']})

    if page == 1:
        startoffset = 0
    else:
        startoffset = (page - 1) * countperpage
    endoffset = startoffset + countperpage

    news=[]
    n = 0
    for user in topic_key_user_list:
        if user:
            n = n + 1
            if n > startoffset:
                if n > endoffset:
                    break
                news.append({'id':user['id'],'name':user['name'],'location':user['location'],'followers_count':user['followers_count'],'bi_followers_count':user['bi_followers_count']})
    total_pages = limit / countperpage + 1
    return json.dumps({'news': news, 'pages': total_pages})
