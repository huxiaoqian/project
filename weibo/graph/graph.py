# -*- coding: utf-8 -*-

import os
import re
import math
import time
import json
import base62
import random
import datetime
import buchheim_weibospread
import requests as pyrequests
from gen_weibospread import Tree
from gexf import Gexf
from lxml import etree
from SSDB import SSDB
from pyelevator import WriteBatch, Elevator
from weibo.global_config import LEVELDBPATH, xapian_search_user, SSDB_PORT, SSDB_HOST
from xapian_weibo.xapian_backend import XapianSearch
from flask import Blueprint, session, redirect, url_for
from weibo.global_config import XAPIAN_RESTFUL_HOST, XAPIAN_RESTFUL_PORT

weibo_fields = ['_id', 'user', 'retweeted_uid', 'retweeted_mid', 'text', 'timestamp', \
                'reposts_count', 'bmiddle_pic', 'geo', \
                'comments_count', 'sentiment']
user_fields = ['_id', 'province', 'city', 'verified', 'name', 'friends_count', \
               'gender', 'profile_image_url', 'verified_type', 'followers_count', \
               'followers', 'location', 'statuses_count', 'friends', 'description', \
               'created_at']

api_host = 'http://%s:%s/status_exist/{mid}' % (XAPIAN_RESTFUL_HOST, XAPIAN_RESTFUL_PORT)
api_host_ts = 'http://%s:%s/status_exist/{mid}/{start_ts}/{end_ts}' % (XAPIAN_RESTFUL_HOST, XAPIAN_RESTFUL_PORT)


def search_status_by_mid(mid):
    # 正常在全库根据mid搜索有无该条微博，有则返回字符串'true', 无则返回'false'
    resp = pyrequests.get(api_host.format(mid=mid))
    resp_json = resp.json()
    if resp.status_code==200 and resp_json['status']=='true':
        print resp_json['data']
        return resp_json['status'], resp_json['data']
    else:
        return 'false', ''


def search_status_by_mid_ts(mid, search_start_ts, search_end_ts):
    # 指定时间段时根据mid搜索有无该条微博，有则返回字符串'true', 无则返回'false'
    resp = pyrequests.get(api_host_ts.format(mid=mid, start_ts=search_start_ts, end_ts=search_end_ts))
    resp_json = resp.json()
    if resp.status_code==200 and resp_json['status']=='true':
        return resp_json['status'], resp_json['data']
    else:
        return 'false', ''


def getWeiboByMid(mid):
    stat, weibo = search_status_by_mid(mid)
    status = {}
    if stat=='true':
        for field in weibo_fields:
            if field == 'user':
                status['user'] = getUserByUid(weibo['user'])
            elif field == 'timestamp':
                status['created_at'] = time.strftime("%a %b %d %H:%M:%S +0800 %Y", time.localtime(weibo['timestamp']))
            else:
                status[field] = weibo[field]
    
    status['id'] = int(mid)
    status['mid'] = str(mid)

    return status


def getUserByUid(uid):
    user = xapian_search_user.search_by_id(int(uid), fields=user_fields)
    user_dict = {}
    if user:
        for field in user_fields:
            user_dict[field] = _utf_8_decode(user[field])
    else:
        user_dict['name'] = str(uid)
        user_dict['location'] = u'未知'
        user_dict['profile_image_url'] = u''

    result_dict = {}
    for k, v in user_dict.iteritems():
        result_dict[k] = v
    result_dict['id'] = int(uid)

    return result_dict


def _utf_8_decode(stri):
    if isinstance(stri, str):
        return unicode(stri, 'utf-8')
    return stri
   

def graph_from_elevator(mid):
    try:
        ssdb = SSDB(SSDB_HOST, SSDB_PORT)
        result = ssdb.request('get', ['weibo_%s' % str(mid)])
        if result.code == 'ok' and result.data:
            return result.data
        return None
    except Exception, e:
        print e
        return None


def forest_from_elevator(topic_id):
    try:
        ssdb = SSDB(SSDB_HOST, SSDB_PORT)
        result = ssdb.request('get', ['topic_%s' % str(topic_id)])
        if result.code == 'ok' and result.data:
            return result.data
        return None
    except Exception, e:
        print e
        return None
