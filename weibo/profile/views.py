# -*- coding: utf-8 -*-

import os
import sys
import urllib2
try:
    import simplejosn as json
except ImportError:
    import json

from flask import Blueprint, url_for, render_template, request, abort, flash

from weibo.extensions import db
import weibo.model
from weibo.model import *

from utils import hot_uid_by_word, last_week, last_month
from flask.ext.sqlalchemy import Pagination


COUNT_PER_PAGE = 20

def getStaticInfo():
    statuscount = db.session.query(RangeCount).filter(RangeCount.countType=='statuses').all()
    friendscount = db.session.query(RangeCount).filter(RangeCount.countType=='friends').all()
    followerscount = db.session.query(RangeCount).filter(RangeCount.countType=='followers').all()
    province = db.session.query(Province).all()
    field = db.session.query(FieldProfile).all()
    return statuscount, friendscount, followerscount, province, field

mod = Blueprint('profile', __name__, url_prefix='/profile')

@mod.route('/search/', methods=['GET', 'POST'])
@mod.route('/search/<model>', methods=['GET', 'POST'])
def profile_search(model='hotest'):
    '''获取最近一周的活跃博主作为热门博主
    '''
    if request.method == 'GET':
        statuscount, friendscount, followerscount, province, field = getStaticInfo()
        if model == 'person':
##            nickname = urllib2.unquote(request.form['nickname'])
##            print nickname
            user = User.query.filter(User.userName=='薛蛮子').all()
            return render_template('profile/profile_search.html',statuscount=statuscount,
                                   friendscount=friendscount, followerscount=followerscount,
                                   location=province, field=field, model=model, result=user)

        else:
        
            return render_template('profile/profile_search.html',statuscount=statuscount,
                                   friendscount=friendscount, followerscount=followerscount,
                                   location=province, field=field, model=model, result=None)
    
    
    if request.method == 'POST' and request.form['page']:
        if model == 'newest':
            basequery = User.query.order_by(User.createdAt.desc())
            page = int(request.form['page'])
            users = basequery.paginate(page, COUNT_PER_PAGE, False).items
            return json.dumps([i.serialize for i in users])
        elif model == 'hotest':
            lowdate, thisdate = last_week()
            uids = hot_uid_by_word(lowdate, thisdate)
            basequery = User.query.filter(User.id.in_(list(uids))).order_by(User.followersCount.desc())
            page = int(request.form['page'])
            users = basequery.paginate(page, COUNT_PER_PAGE, False).items
            return json.dumps([i.serialize for i in users])
        elif db.session.query(FieldProfile).filter(FieldProfile.fieldEnName==model).count():
            uids = db.session.query(UserField.uid).filter(UserField.fieldFirst==model).all()
            uidlist = [uid[0] for uid in uids]
            basequery = User.query.filter(User.id.in_(uidlist)).order_by(User.followersCount.desc())
            page = int(request.form['page'])
            users = basequery.paginate(page, COUNT_PER_PAGE, False).items
            return json.dumps([i.serialize for i in users])
##        elif db.session.query(RangeCount).filter(RangeCount.countType==model).count():
##            pass
        elif model == 'person':
            nickname = urllib2.unquote(request.form['nickname'])
            print nickname
            basequery = User.query.filter(User.userName==nickname).all()
            page = int(request.form['page'])
            users = basequery.paginate(page, COUNT_PER_PAGE, False).items
            return json.dumps([i.serialize for i in users])

@mod.route('/group/<fieldEnName>', methods=['GET', 'POST'])
def profile_group(fieldEnName='finance'):
    field = FieldProfile.query.all()
    return render_template('profile/profile_group.html', field=field, model=fieldEnName)

@mod.route('/person/<uid>', methods=['GET', 'POST'])
def profile_person(uid):
    if uid:
        user = User.query.filter_by(id=long(uid)).first_or_404()
    return render_template('profile/profile_person.html', user=user)

@mod.route('/person_tab_ajax/<model>/<uid>')
def profile_person_tab_ajax(model, uid):
    if model == 'personaltopic':
        return render_template('profile/ajax/personal_word_cloud.html', uid=uid)
    elif model == 'personalweibocount':
        return render_template('profile/ajax/personal_weibo_count.html', uid=uid)
    elif model == 'personalnetwork':
        return render_template('profile/ajax/personal_network.html', uid=uid)
    elif model == 'personal_friends_followers':
        result = []
        users = UserField.query.limit(10000)
        for user in users:
##            uids = 
            raw = User.query.filter_by(id=user.uid).order_by(User.statusesCount.desc()).first()
            if raw:
                result.append({'id': raw.id,'userName': raw.userName, 'statusesCount': raw.statusesCount,
                               'followersCount': raw.followersCount, 'friendsCount': raw.friendsCount,
                               'field': user.fieldFirst})
        return render_template('profile/ajax/personal_friends_followers.html', result=result[:9], uid=uid)
    elif model == 'grouptopic':
        return render_template('profile/ajax/group_word_cloud.html')
    elif model == 'groupweibocount':
        return render_template('profile/ajax/group_weibo_count.html')
    elif model == 'grouprank':
        result = []
        users = UserField.query.filter_by(fieldEnName='finance').limit(10000)
        for user in users:
            result.extend(User.query.filter_by(id=user.uid).order_by(User.statusesCount.desc()))
        return render_template('profile/ajax/group_rank_bloggers.html', result=result[:9])
    elif model == 'grouplocation':
        return render_template('profile/ajax/group_location.html')
    elif model == 'groupactive':
        return render_template('profile/ajax/group_active.html')
    elif model == 'groupemotion':
        return render_template('profile/ajax/group_emotion.html')

@mod.route('/person_topic/<timeinterval>/<uid>')
def profile_person_topic(uid,timeinterval='onemonth'):
    from datetime import datetime
    from burst_word import date2ts
    result_arr = []
    lowdate, thisdate = last_week()
    if timeinterval == 'onemonth':
        results = PersonalBurstWords.query.filter(PersonalBurstWords.uid == uid, PersonalBurstWords.windowSize == 24*60*60,
                                                  PersonalBurstWords.startDate>=datetime.fromtimestamp(date2ts(lowdate)),
                                                  PersonalBurstWords.endDate<=datetime.fromtimestamp(date2ts(thisdate))).all()
    elif timeinterval == 'oneweek':
        results = PersonalBurstWords.query.filter(PersonalBurstWords.uid == uid, PersonalBurstWords.windowSize == 24*60*60,
                                                  PersonalBurstWords.startDate>=datetime.fromtimestamp(date2ts(lowdate)),
                                                  PersonalBurstWords.endDate<=datetime.fromtimestamp(date2ts(thisdate))).all()
    for result in results:
        result_arr.append({'text': result.word, 'size': result.freq})
    return json.dumps(result_arr)

@mod.route('/person_count/<timeinterval>/<uid>')
def personal_weibo_count(uid, timeinterval):
    from utils import ts2datetime, time2ts
    from sqlalchemy import func
    from datetime import date
    result_arr = []
    lowdate, thisdate = last_month()
    startdate = ts2datetime(time2ts(lowdate))
    enddate =  ts2datetime(time2ts(thisdate))
    results = db.session.query(func.year(Words.postDate), func.month(Words.postDate), func.day(Words.postDate), func.count(Words.id)).\
              filter(Words.uid==long(uid), Words.postDate>startdate, Words.postDate<enddate).\
              group_by(func.year(Words.postDate), func.month(Words.postDate), func.day(Words.postDate)).all()
    time_arr = []
    count_arr = []
    repost_arr = []
    fipost_arr = []
    for year, month, day, count in results:
        time_arr.append(date(year, month, day).isoformat())
        count_arr.append(count)
        fipost_arr.append(1)
        repost_arr.append(count-1)
    return json.dumps({'time': time_arr, 'count': count_arr, 'repost': repost_arr, 'fipost': fipost_arr})

@mod.route('/result.json')
def graph_result():
    return json.dumps(graph_result)
graph_result = {"nodes":[{"name":"薛蛮子","group":0,"profile_image_url":"0","domain":"社会"},                         
                         {"name":"内分泌顾锋","group":1,"profile_image_url":"1","domain":"报刊"},
                         {"name":"留几手","group":1,"profile_image_url":"2","domain":"IT"},
                         {"name":"妈咪Jane育儿妙方","group":1,"profile_image_url":"3","domain":"传媒"},
                         {"name":"好友美食","group":2,"profile_image_url":"4","domain":"社会"},
                         {"name":"毛高山","group":2,"profile_image_url":"5","domain":"财经"},
                         {"name":"管鹏","group":2,"profile_image_url":"6","domain":"社会"},
                         {"name":"协和章蓉娅","group":3,"profile_image_url":"7","domain":"教育"},
                         {"name":"张遇升","group":3,"profile_image_url":"8","domain":"社会"},
                         {"name":"炎黄春秋编辑部","group":3,"profile_image_url":"9","domain":"社会"},
                         {"name":"苏家桥","group":4,"profile_image_url":"10","domain":"社会"},
                         {"name":"精神科李医生","group":4,"profile_image_url":"11","domain":"社会"},
                         {"name":"最活跃的关注者","group":-1,"profile_image_url":"1-1","domain":""},
                         {"name":"联系最紧密的关注者","group":-2,"profile_image_url":"1-1","domain":""},
                         {"name":"最活跃的粉丝","group":-3,"profile_image_url":"1-1","domain":""},
                         {"name":"联系最紧密的粉丝","group":-4,"profile_image_url":"1-1","domain":""},
                         {"name":"释源祖庭白马寺","group":5,"profile_image_url":"12","domain":""},
                         {"name":"Brad_Pitt","group":5,"profile_image_url":"13","domain":""},
                         {"name":"胡泽涛-Henry","group":5,"profile_image_url":"14","domain":""},
                         {"name":"姚树坤","group":5,"profile_image_url":"15","domain":""},
                         {"name":"瞬间就笑岔气了","group":5,"profile_image_url":"16","domain":""},
                         {"name":"口袋悦读","group":5,"profile_image_url":"17","domain":""},
                         {"name":"慕容雪村","group":5,"profile_image_url":"18","domain":""},
                         {"name":"老树画画","group":5,"profile_image_url":"19","domain":""},
                         {"name":"大河网信阳频道吴彦飞","group":5,"profile_image_url":"20","domain":""},
                         {"name":"桔子水晶吴海","group":5,"profile_image_url":"21","domain":""},
                         {"name":"谢维冰","group":5,"profile_image_url":"22","domain":""},
                         {"name":"社科院杨团","group":5,"profile_image_url":"23","domain":""},
                         {"name":"围脖唯美句","group":5,"profile_image_url":"24","domain":""},
                         {"name":"参考消息","group":5,"profile_image_url":"25","domain":""},
                         {"name":"九个头条","group":5,"profile_image_url":"26","domain":""},
                         {"name":"天才小熊猫","group":5,"profile_image_url":"27","domain":""},
                         {"name":"赵克罗","group":5,"profile_image_url":"28","domain":""},
                         {"name":"泰国苏梅岛自助游指南","group":5,"profile_image_url":"29","domain":""},
                         {"name":"苏宁孙为民","group":5,"profile_image_url":"30","domain":""},
                         {"name":"马翾","group":5,"profile_image_url":"31","domain":""}],
"links":[{"source":0,"target":12,"value":20},{"source":0,"target":13,"value":16},{"source":0,"target":14,"value":18},{"source":0,"target":15,"value":18},
         {"source":12,"target":1,"value":2},{"source":12,"target":2,"value":8},{"source":12,"target":3,"value":10},{"source":13,"target":4,"value":6},
         {"source":13,"target":5,"value":2},{"source":13,"target":6,"value":8},{"source":14,"target":7,"value":10},{"source":14,"target":8,"value":6},
         {"source":14,"target":9,"value":2},{"source":15,"target":10,"value":8},{"source":15,"target":11,"value":10},{"source":0,"target":16,"value":0.5},
         {"source":0,"target":17,"value":0.5},{"source":0,"target":18,"value":0.5},{"source":0,"target":19,"value":0.5},{"source":0,"target":20,"value":0.5},
         {"source":0,"target":21,"value":0.5},{"source":0,"target":22,"value":0.5},{"source":0,"target":23,"value":0.5},{"source":0,"target":24,"value":0.5},
         {"source":0,"target":25,"value":0.5},{"source":0,"target":26,"value":0.5},{"source":0,"target":27,"value":0.5},{"source":0,"target":28,"value":0.5},
         {"source":0,"target":29,"value":0.5},{"source":0,"target":30,"value":0.5},{"source":0,"target":31,"value":0.5},{"source":0,"target":32,"value":0.5},
         {"source":0,"target":33,"value":0.5},{"source":0,"target":34,"value":0.5},{"source":0,"target":35,"value":0.5}]}
