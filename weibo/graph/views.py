# -*- coding: utf-8 -*-

import json
import math
import time
from datetime import date
from datetime import datetime
from graph import getWeiboByMid, graph_from_elevator, forest_from_elevator
from flask import Blueprint, session, render_template, redirect, \
                  url_for, jsonify, make_response, request
from get_result import *

mod = Blueprint('graph', __name__, url_prefix='/gexf')

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

@mod.route('/show_graph/<int:mid>/<module>/')
@mod.route('/show_graph/<int:mid>/<int:page>/<module>/')
def show_graph_index(mid, module, page=None):

    if page is None:
        per_page = 200
        # weibo
        weibo = getWeiboByMid(mid)
        try:
            retweeted_mid = weibo['retweeted_mid']
        except:
            return json.dumps('No such mid')

        # source_weibo
        source_weibo = weibo
        if retweeted_mid != 0:
            source_weibo = getWeiboByMid(retweeted_mid)

        #reposts_count = source_weibo['reposts_count']
        #total_page = int(math.ceil(reposts_count * 1.0 / per_page))
        #page = total_page
        page = 0

        return redirect('/gexf/show_graph/%s/%s/%s'%(mid, page, module))#{url_for(graph.show_graph(mid, page))})

    screen_name = 'nobody'
    profile_image_url = 'http://www.baidu.com'
    module = str(module)

    blog_info = readPropagateSingle(mid)#返回整个树的统计

    if blog_info:
        if blog_info[0]['profile_image_url'] == 'None':
            blog_img_url = ''
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
            blog_img_url = ''
        else:
            blog_img_url = blog_info[0]['profile_image_url']
        bloger_name = blog_info[0]['name']
        blog_reposts_count = blog_info[0]['repostsCount']
        blog_comments_count = blog_info[0]['commentsCount']
        blog_attitudes_count = blog_info[0]['attitudesCount']
        blog_time = blog_info[0]['postDate']
        blog_text = blog_info[0]['text']
    
    if module == 'sub':
        return render_template('graph/graph.html', btnuserpicvisible='inline',
                               btnloginvisible='none',
                               screen_name=screen_name, profile_image_url=profile_image_url,
                               tar_profile_image_url = blog_img_url,
                               tar_screen_name = bloger_name,
                               tar_repost_count = blog_reposts_count,
                               tar_comments_count = blog_comments_count,
                               tar_attitudes_count = blog_attitudes_count,
                               tar_post_date = blog_time,
                               tar_text = blog_text,
                               mid=mid,
                               page=page)
    else:
        return render_template('graph/graph_whole.html', btnuserpicvisible='inline',
                               btnloginvisible='none',
                               screen_name=screen_name, profile_image_url=profile_image_url,
                               tar_profile_image_url = blog_img_url,
                               tar_screen_name = bloger_name,
                               tar_repost_count = blog_reposts_count,
                               tar_comments_count = blog_comments_count,
                               tar_attitudes_count = blog_attitudes_count,
                               tar_post_date = blog_time,
                               tar_text = blog_text,
                               mid=mid,
                               page=page)

@mod.route('/graph/<int:mid>/')
@mod.route('/graph/<int:mid>/<int:page>/')
def graph_index(mid, page=None):
    module = str(request.args.get('module'))
    g = graph_from_elevator(mid)

    if not g:
        g = ''

    result = g.split('_\/')
    if len(result) > 1:
        if module == 'sub':
            tree_g = result[2]
            tree_stats = json.loads(result[3])
        else:
            tree_g = result[0]
            tree_stats = json.loads(result[1])

        response = make_response(tree_g)
        response.headers['Content-Type'] = 'text/xml'

        return response
    else:
        response = make_response('')
        response.headers['Content-Type'] = 'text/xml'

        return response

@mod.route('/tree_stats/<int:mid>/<int:page>/')
def tree_stats_index(mid, page):
    module = str(request.args.get('module'))
    g = graph_from_elevator(mid)

    if not g:
        g = ''

    result = g.split('_\/')
    if len(result) > 1:
        if module == 'sub':
            tree_g = result[2]
            tree_stats = json.loads(result[3])
        else:
            tree_g = result[0]
            tree_stats = json.loads(result[1])
    else:
        tree_stats = {}

    tree_stats['spread_begin'] = ts2HMS(tree_stats['spread_begin'])#tree_stats['spread_begin'].strftime('%Y-%m-%d %H:%M:%S')
    tree_stats['spread_end'] = ts2HMS(tree_stats['spread_end'])#tree_stats['spread_end'].strftime('%Y-%m-%d %H:%M:%S')

    return jsonify(stats=tree_stats)


def ts2HMS(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))

@mod.route('/show_forest/')
def show_forest_index():
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
        if topic_img_url == 'None':
            topic_profile_image_url = ''
        else:
            topic_profile_image_url = topic_img_url

    dur_time = _utf_decode(dur_time)

    screen_name = 'nobody'
    profile_image_url = 'http://www.baidu.com'
    return render_template('graph/forest.html', btnuserpicvisible='inline',
                           btnloginvisible='none',
                           screen_name=screen_name, profile_image_url=profile_image_url,
                           topic_profile_image_url = topic_profile_image_url,
                           topic_ori_screen_name = topic_ori_screen_name,
                           blog_rel_count = topic_blog_count,
                           blog_ori_count = topic_blog_ori_count,
                           blog_ori_account = blog_ori_account,
                           topic_leader_count = topic_leader_count,
                           topic_ori_date = topic_ori_date,
                           keyword=keyword, topic_id=topic_id, dur_time=dur_time,
                           return_beg_str=return_beg_str, return_end_str=return_end_str)

@mod.route('/forest/<int:topic_id>/')
def forest_index(topic_id):
    g = forest_from_elevator(topic_id)

    if g:
        response = make_response(g)
        response.headers['Content-Type'] = 'text/xml'

        return response
    else:
        response = make_response('')
        response.headers['Content-Type'] = 'text/xml'

        return response
