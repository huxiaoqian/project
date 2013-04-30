# -*- coding: UTF-8 -*-

'''微博主题数量统计和突发词提取
'''

import os
import json
import codecs
import re
import time
import math
import csv
from datetime import date

def date2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def local2date(time_str):
    time_format = '%a %b %d %H:%M:%S +0800 %Y'
    return date.fromtimestamp(int(time.mktime(time.strptime(time_str, time_format))))

def calculateStatus(uidnumber=1813080181):
    reader_user = csv.reader(file('./data/spring_weibo_20130331.csv', 'r'))
    count = 0
    date_status = {}
    for line in reader_user:
        mid, uid, reposts, retweeted_mid, retweeted_uid, retweeted_name, retweeted_ts, retweeted_text, retweeted_geo, retweeted_reposts_count, retweeted_attitudes_count, retweeted_source_plat, retweeted_comments_count, geo, reposts_count, text, post_time, source_plat, attitudes_count, comments_count, bmiddle_pic = line
        uid = eval(uid)
        mid = eval(mid)
        
        if count >= 1 and uid == uidnumber:
            post_time = local2date(post_time)
            if retweeted_mid == 'none':
                repost = 0
            else:
                repost = 1
            try:
                status = date_status[post_time]
                status.append(repost)
            except KeyError,e:
                date_status[post_time] = [repost]
        count += 1
        if count % 100000 == 0:
            print count
    date_status_list = sorted(date_status.iteritems(), key=lambda(k, v): k)
    date_arr = []
    total_arr = []
    repost_arr = []
    first_arr = []
    for date, status in date_status_list:
        result = {0: 0, 1:0}
        total = 0
        date_arr.append(date.strftime("%Y-%m-%d"))
        for s in status:
            result[s] += 1
            total += 1
        total_arr.append(total)
        repost_arr.append(result[1])
        first_arr.append(result[0])
    print date_arr, total_arr, repost_arr, first_arr
    return json.dumps([date_arr, total_arr, repost_arr, first_arr])

def getCount():  
    result_dict = {}
    
    line_index = 0
    total_num = 0
    repost_num = 0
    user_num = 0
    date_arr = []
    total_arr = []
    repost_arr = []
    user_arr = []
    with open("./static/data/weibo_count.txt") as f:
        for line in f:
            if line_index > 0:
                date, total_count, repost_count, user_count = line.strip().split("\t")
                total_num +=  int(total_count)
                repost_num +=  int(repost_count)
                user_num +=  int(user_count)
                date_arr.append(date)
                total_arr.append(int(total_count))
                repost_arr.append(int(repost_count))
                user_arr.append(int(user_count))
            line_index += 1 
    result_dict = {'total_num': total_num, 'repost_num': repost_num, 'user_num': user_num, 'date_arr': date_arr, 'total_arr': total_arr,
                   'repost_arr': repost_arr, 'user_arr': user_arr}
    return result_dict

def getBurstWord():
    result_dict = {}
    path = os.path.join(os.getcwd(), "./static/data/")
    with codecs.open(os.path.join(path, 'results_2012-9-1_2012-10-1_24.txt'), 'r', encoding='utf-8') as f:
        for line in f.readlines():
            words = line.strip().split(" ")
            word = words[0]
            burst= float(words[1])
            result_dict[word] = burst
    sortbyburst = sorted(result_dict.iteritems(), key=lambda(k, v): v)
    return sortbyburst

def groupBurstWord():
    path = os.path.join(os.getcwd(), "./data/burst")
    for i in os.listdir(path):
        match = re.match('summary_results_(.*)_.*_.*\.txt', i)
        
        if match:
            fn = match.group(0)
            day_dict = {}
            with codecs.open(os.path.join(path, fn), 'r', encoding='utf-8') as f:
                for line in f.readlines():
                    words = line.strip().split(" ")
                    word = words[0]
                    burst= float(words[1])
                    freq = int(words[2])
                    day_dict[word] = [burst, freq]
            return day_dict

def chinamap():
    raw_data = {'北京': 345, '上海': 354, '重庆': 453, '天津': 343, '河北': 34, '河南': 54, '黑龙江': 53, '辽宁': 33}
    china_map_count, city_sorted = map_count(raw_data)
    return china_map_count

def map_count(raw_data):
    print province_color_map(raw_data)
    return province_color_map(raw_data)

def province_color_map(city_count):
    total_count = sum(city_count.values())
    city_sorted = sorted(city_count.iteritems(), key=lambda(k, v): v, reverse=True)
    city_color = {}
    city_count = {}
    city_summary = []
    #color = ['#000079', '#0f1486', '#1e2893', '#2d3ca1', '#3c51ae', '#4b65bc', '#5a79c9', '#698ed6', '#78a2e4', '#87b6f1', '#96cafe']
    #color = ['#2873AC', '#29AC80', '#51B133', '#FFD914', '#FD8D24', '#FF5B25']
    color = ['#1034f0', '#2231ee', '#342eec', '#462bea', '#5829e8', '#6a26e6', '#7c23e4', '#8e20e3', '#a01de1', '#b21adf', '#c418dd', '#d615db', '#e812d9', '#fa0fd7']
    color.reverse()
    if len(city_sorted) > len(color):
        n = int(math.ceil(len(city_sorted)*1.0/len(color)))
        for i in range(0, len(city_sorted), n):
            for j in range(n):
                if i+j < len(city_sorted):
                    city, count = city_sorted[i+j]
                    if count == 0:
                        continue
                    city_color[city] = color[i/n]
                    rank = i+j+1
                    percent = str(int(count*1000/total_count)/10.0)+'%'
                    if rank <= 10:
                        city_summary.append([rank, city, percent])
                    city_count[city] = [count, rank, percent]
    else:
        for index, x in enumerate(city_sorted):
            if total_count:
                city, count = x
                city_color[city] =  "%s" % color[index]
                percent = str(int(count*1000/total_count)/10.0)+'%'
                rank = index+1
                if rank <= 10:
                    city_summary.append([rank, city, percent])
                city_count[city] = [count, rank, percent]
    data = {'count': city_count,
            'color': city_color,
            'summary': city_summary}
    city_sorted = [(city, [0, 0, 0]) for city, count in city_sorted]
    return data, city_sorted

if __name__ == '__main__':
    #getCountFromTxt()
    #getBurstWord()
    #chinamap()
    calculateStatus()
    
    
