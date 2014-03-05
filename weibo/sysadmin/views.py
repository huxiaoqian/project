# -*- coding: utf-8 -*-

from flask import Blueprint, url_for, render_template, request, abort, flash, session, redirect
import weibo.model
from weibo.model import *
from weibo.extensions import db
import json
import csv
import os
import time
from xapian_weibo.xapian_backend import XapianSearch
from weibo.global_config import xapian_search_user
from read_log import *
#xapian_search_user = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)

mod = Blueprint('sysadmin', __name__, url_prefix='/sysadmin')

@mod.route('/')
@mod.route('/login', methods=['GET','POST'])
def index():
    result = None
    if 'logged_in' in session and session['logged_in']:
        session.pop('user', None)
        session.pop('logged_in', None)
    if request.method == 'POST':
        pas = db.session.query(Manager).filter(Manager.managerName==request.form['user']).all()
        if pas != []:
            for pa in pas:
                if request.form['para'] == pa.password:
                    result = 'Right'
            if result == 'Right':
                session['logged_in'] = 'Ture'
                session['user'] = request.form['user']
                flash('登录成功！')
            else:
                result = 'Wrong_pass'
        return json.dumps(result)

    return render_template('admin/index.html', result = result)

@mod.route('/paraset/field/')
def help_field():
    if 'logged_in' in session and session['logged_in']:
        return render_template('admin/para_field.html') 
    else:
        return redirect('/sysadmin/')

@mod.route('/paraset/new/')
def help_new():
    if 'logged_in' in session and session['logged_in']:
        return render_template('admin/para_new.html') 
    else:
        return redirect('/sysadmin/')

@mod.route('/paraset/userlist/')
def help_userlist():
    if 'logged_in' in session and session['logged_in']:
        userlists = db.session.query(UserList).filter().all()
        return render_template('admin/para_userlist.html', userlists = userlists) 
    else:
        return redirect('/sysadmin/')

@mod.route('/paraset/weight/')
def help_weight():
    if 'logged_in' in session and session['logged_in']:
        userweights = db.session.query(UserWeight).filter().all()
        return render_template('admin/para_weight.html',userweights = userweights) 
    else:
        return redirect('/sysadmin/')

@mod.route('/paraset/black/')
def help_black():
    if 'logged_in' in session and session['logged_in']:
        return render_template('admin/para_hei.html') 
    else:
        return redirect('/sysadmin/')

@mod.route('/paraset/media/')
def help_media():
    if 'logged_in' in session and session['logged_in']:
        return render_template('admin/para_media.html') 
    else:
        return redirect('/sysadmin/')

@mod.route('/paraset/material/')
def help_material():
    if 'logged_in' in session and session['logged_in']:
        return render_template('admin/para_ma.html') 
    else:
        return redirect('/sysadmin/')

@mod.route('/paraset/topic/')
def help_topic():
    if 'logged_in' in session and session['logged_in']:
        return render_template('admin/para_topic.html') 
    else:
        return redirect('/sysadmin/')

@mod.route('/paraset/topic_history/')
def help_history():
    if 'logged_in' in session and session['logged_in']:
        return render_template('admin/para_history.html') 
    else:
        return redirect('/sysadmin/')

@mod.route('/paraset/log/')
def help_log():
    if 'logged_in' in session and session['logged_in']:
        return render_template('admin/para_log.html') 
    else:
        return redirect('/sysadmin/')

@mod.route('/usermanage/')
def contacts():
    if  'logged_in' in session and session['logged_in']:
        information = db.session.query(Manager).filter().all()
        return render_template('admin/user_manage.html', information = information)    
    else:
        return redirect('/sysadmin/')

@mod.route('/logout')
def logout():
    session.pop('user', None)
    session.pop('logged_in', None)
    flash('退出成功！')
    return redirect('/sysadmin/')

@mod.route('/change_pass', methods=['GET','POST'])
def ch_pass():
    result = None
    pas = db.session.query(Manager).filter(Manager.managerName==session['user']).all()
    old_pass = request.form['old_pa']
    new_pass = request.form['new_pa']
    old_pass = old_pass.strip('@\r\n\t')
    new_pass = new_pass.strip('@\r\n\t')
    for pa in pas:
        if old_pass == pa.password:
            result = 'Right'
            old_items = db.session.query(Manager).filter(Manager.password==old_pass).all()
            for old_item in old_items:
                db.session.delete(old_item)
                db.session.commit()
            new_item = Manager(password=new_pass,managerName=session['user'])
            db.session.add(new_item)
            db.session.commit()
            return json.dumps(result)
    return json.dumps(result)

@mod.route('/add_field', methods=['GET','POST'])
def add_field():
    result = 'Right'
    new_field = request.form['topic']
    old_items = db.session.query(Field).filter(Field.fieldName==new_field).all()
    if len(old_items):
        result = 'Wrong'
    else:
        new_item = Field(fieldName=new_field)
        db.session.add(new_item)
        db.session.commit()
    return json.dumps(result)

@mod.route('/add_new', methods=['GET','POST'])
def add_new():
    result = 'Right'
    new_field = request.form['topic']
    se_weight = request.form['se_weight']
    old_items = db.session.query(NewWords).filter(NewWords.wordsName==new_field).all()
    if len(old_items):
        result = 'Wrong'
    else:
        new_item = NewWords(wordsName=new_field,seWeight=se_weight)
        db.session.add(new_item)
        db.session.commit()
    return json.dumps(result)


@mod.route('/add_black', methods=['GET','POST'])
def add_black():
    result = 'Right'
    new_field = request.form['topic']
    count, get_results = xapian_search_user.search(query={'_id': new_field}, fields=['_id', 'name'])
    if count > 0:
        for get_result in get_results():
            new_item = BlackList(blackID=get_result['_id'],blackName=get_result['name'])
            db.session.add(new_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route('/add_media', methods=['GET','POST'])
def add_media():
    result = 'Right'
    new_field = request.form['topic']
    count, get_results = xapian_search_user.search(query={'_id': new_field}, fields=['_id', 'name'])
    if count > 0:
        for get_result in get_results():
            new_item = IMedia(mediaID=get_result['_id'],mediaName=get_result['name'])
            db.session.add(new_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route('/change_weight', methods=['GET','POST'])
def change_weight():
    result = 'Right'
    new_weight = request.form['topic']
    weight_name = request.form['we_name']
    old_items = db.session.query(UserWeight).filter(UserWeight.weightName==weight_name).all()
    for old_item in old_items:
        db.session.delete(old_item)
        db.session.commit()
        new_item = UserWeight(weightName=weight_name,weight=new_weight)
        db.session.add(new_item)
        db.session.commit()
    return json.dumps(result)

@mod.route('/field_de', methods=['GET','POST'])
def field_de():
    result = 'Right'
    new_id = request.form['f_id']       
    old_items = db.session.query(Field).filter(Field.id==new_id).all()
    if len(old_items):
        for old_item in old_items:
            db.session.delete(old_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route('/new_de', methods=['GET','POST'])
def new_de():
    result = 'Right'
    new_id = request.form['f_id']
    old_items = db.session.query(NewWords).filter(NewWords.id==new_id).all()
    if len(old_items):
        for old_item in old_items:
            db.session.delete(old_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)


@mod.route('/hei_de', methods=['GET','POST'])
def hei_de():
    result = 'Right'
    new_id = request.form['f_id']
    old_items = db.session.query(BlackList).filter(BlackList.blackID==new_id).all()
    if len(old_items):
        for old_item in old_items:
            db.session.delete(old_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route('/media_de', methods=['GET','POST'])
def media_de():
    result = 'Right'
    new_id = request.form['f_id']
    old_items = db.session.query(IMedia).filter(IMedia.mediaID==new_id).all()
    if len(old_items):
        for old_item in old_items:
            db.session.delete(old_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route('/material_de', methods=['GET','POST'])
def material_de():
    result = 'Right'
    new_id = request.form['f_id']
    old_items = db.session.query(M_Weibo).filter(M_Weibo.weibo_id==new_id).all()
    if len(old_items):
        for old_item in old_items:
            db.session.delete(old_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route('/new_in', methods=['GET','POST'])
def new_in():
    f_name = request.form['new_words']
    flag = request.form['flag']
    if flag == '1':
        line = f_name.split('\n')
        for li in line:
            item = li.split(',')
            n = 0
            name = 'na'
            weight = 'we'
            for it in item:
                if n == 0:
                    name = it
                    n = 1
                else:
                    weight = it
            old_items = db.session.query(NewWords).filter(NewWords.wordsName==name.encode('utf-8')).all()
            if len(old_items):
                pass
            else:
                new_item = NewWords(wordsName=name.encode('utf-8'),seWeight=weight)
                db.session.add(new_item)
                db.session.commit()
    else:
        line = f_name.split('\n')
        for li in line:
            name = li           
            old_items = db.session.query(NewWords).filter(NewWords.wordsName==name.encode('utf-8')).all()
            if len(old_items):
                pass
            else:
                new_item = NewWords(wordsName=name.encode('utf-8'),seWeight=1)
                db.session.add(new_item)
                db.session.commit()

    if len(f_name) > 0:
        result = 'Right'
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route('/rank/')
def newwords_rank():
    page = 1
    countperpage = 10
    limit = 1000000
    if request.args.get('page'):
        page = int(request.args.get('page'))
    if request.args.get('countperpage'):
        countperpage = int(request.args.get('countperpage'))
    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
    if page == 1:
        startoffset = 0
    else:
        startoffset = (page - 1) * countperpage
    endoffset = startoffset + countperpage
    newwords = db.session.query(NewWords).filter().all()
    news=[]
    n = 0
    for newword in newwords:
        if newword:
            n = n + 1
            if n > startoffset:
                if n > endoffset:
                    break 
                news.append({'id':newword.id,'wordsName':newword.wordsName.encode('utf-8'),'seWeight':newword.seWeight})
    total_pages = limit / countperpage + 1
    return json.dumps({'news': news, 'pages': total_pages})

@mod.route('/material_rank/')
def material_rank():
    page = 1
    countperpage = 10
    limit = 1000000
    if request.args.get('page'):
        page = int(request.args.get('page'))
    if request.args.get('countperpage'):
        countperpage = int(request.args.get('countperpage'))
    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
    if page == 1:
        startoffset = 0
    else:
        startoffset = (page - 1) * countperpage
    endoffset = startoffset + countperpage
    newwords = db.session.query(M_Weibo).filter().all()
    news=[]
    n = 0
    for newword in newwords:
        if newword:
            n = n + 1
            if n > startoffset:
                if n > endoffset:
                    break 
                news.append({'weibo_id':newword.weibo_id,'text':newword.text.encode('utf-8'),'repostsCount':newword.repostsCount,'commentsCount':newword.commentsCount,'postDate':str(newword.postDate),'uid':newword.uid})
    total_pages = limit / countperpage + 1
    return json.dumps({'news': news, 'pages': total_pages})

@mod.route('/hei_rank/')
def hei_rank():
    page = 1
    countperpage = 10
    limit = 1000000
    if request.args.get('page'):
        page = int(request.args.get('page'))
    if request.args.get('countperpage'):
        countperpage = int(request.args.get('countperpage'))
    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
    if page == 1:
        startoffset = 0
    else:
        startoffset = (page - 1) * countperpage
    endoffset = startoffset + countperpage
    newwords = db.session.query(BlackList).filter().all()
    news=[]
    n = 0
    for newword in newwords:
        if newword:
            n = n + 1
            if n > startoffset:
                if n > endoffset:
                    break 
                news.append({'id':newword.id,'blackName':newword.blackName.encode('utf-8'),'blackID':newword.blackID})
    total_pages = limit / countperpage + 1
    return json.dumps({'news': news, 'pages': total_pages})

@mod.route('/media_rank/')
def media_rank():
    page = 1
    countperpage = 10
    limit = 1000000
    if request.args.get('page'):
        page = int(request.args.get('page'))
    if request.args.get('countperpage'):
        countperpage = int(request.args.get('countperpage'))
    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
    if page == 1:
        startoffset = 0
    else:
        startoffset = (page - 1) * countperpage
    endoffset = startoffset + countperpage
    newwords = db.session.query(IMedia).filter().all()
    news=[]
    n = 0
    for newword in newwords:
        if newword:
            n = n + 1
            if n > startoffset:
                if n > endoffset:
                    break 
                news.append({'id':newword.id,'mediaName':newword.mediaName.encode('utf-8'),'mediaID':newword.mediaID})
    total_pages = limit / countperpage + 1
    return json.dumps({'news': news, 'pages': total_pages})

@mod.route('/f_rank/')
def f_rank():
    page = 1
    countperpage = 5
    limit = 1000000
    if request.args.get('page'):
        page = int(request.args.get('page'))
    if request.args.get('countperpage'):
        countperpage = int(request.args.get('countperpage'))
    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
    if page == 1:
        startoffset = 0
    else:
        startoffset = (page - 1) * countperpage
    endoffset = startoffset + countperpage
    newwords = db.session.query(Field).filter().all()
    news=[]
    n = 0
    for newword in newwords:
        if newword:
            n = n + 1
            if n > startoffset:
                if n > endoffset:
                    break 
                news.append({'id':newword.id,'fieldName':newword.fieldName.encode('utf-8')})
    total_pages = limit / countperpage + 1
    return json.dumps({'news': news, 'pages': total_pages})

@mod.route('/topic_rank/')
def topic_rank():
    page = 1
    countperpage = 5
    limit = 1000000
    if request.args.get('page'):
        page = int(request.args.get('page'))
    if request.args.get('countperpage'):
        countperpage = int(request.args.get('countperpage'))
    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
    if page == 1:
        startoffset = 0
    else:
        startoffset = (page - 1) * countperpage
    endoffset = startoffset + countperpage
    local = int(time.time())
    topics = db.session.query(Topics).filter((Topics.iscustom==True)&(Topics.expire_date>=local)).all()
    news=[]
    n = 0
    for topic in topics:
        if topic:
            n = n + 1
            if n > startoffset:
                if n > endoffset:
                    break
                expire_date = time.strftime('%Y-%m-%d', time.localtime(topic.expire_date))
                db_date = time.strftime('%Y-%m-%d', time.localtime(topic.db_date))
                news.append({'id':topic.id,'topic':topic.topic.encode('utf-8'),'expire_date':expire_date,'enter_data':db_date})
    total_pages = limit / countperpage + 1
    return json.dumps({'news': news, 'pages': total_pages})

@mod.route('/history_rank/')
def history_rank():
    page = 1
    countperpage = 5
    limit = 1000000
    if request.args.get('page'):
        page = int(request.args.get('page'))
    if request.args.get('countperpage'):
        countperpage = int(request.args.get('countperpage'))
    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
    if page == 1:
        startoffset = 0
    else:
        startoffset = (page - 1) * countperpage
    endoffset = startoffset + countperpage
    local = int(time.time())
    topics = db.session.query(TopicStatus).filter((TopicStatus.status==0)|(TopicStatus.status==1)|(TopicStatus.status==-1)).all()
    news=[]
    n = 0
    for topic in topics:
        if topic:
            n = n + 1
            if n > startoffset:
                if n > endoffset:
                    break
                start = time.strftime('%Y-%m-%d', time.localtime(topic.start))
                end = time.strftime('%Y-%m-%d', time.localtime(topic.end))
                db_date = time.strftime('%Y-%m-%d', time.localtime(topic.db_date))
                news.append({'id':topic.id,'topic':topic.topic.encode('utf-8'),'start':start,'end':end,'status':topic.status,'enter_data':db_date})
    total_pages = limit / countperpage + 1
    return json.dumps({'news': news, 'pages': total_pages})

@mod.route('/add_user', methods=['GET','POST'])
def add_user():
    user = request.form['user']
    pas = request.form['pass']
    identify = request.form['identify']
    moodlens = request.form['moodlens']
    profile = request.form['profile']
    propagate = request.form['propagate']
    old_items = db.session.query(UserList).filter(UserList.username==user).all()
    if len(old_items):
        return json.dumps('Wrong')
    else:
        new_item = UserList(username=user,password=pas,identify=identify,moodlens=moodlens,profile=profile,propagate=propagate)
        db.session.add(new_item)
        db.session.commit()
        return json.dumps('Right')

@mod.route('/user_de', methods=['GET','POST'])
def user_de():
    result = 'Right'
    user_id = request.form['f_id']
    old_items = db.session.query(UserList).filter(UserList.id==user_id).all()
    if len(old_items):
        for old_item in old_items:
            db.session.delete(old_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route('/user_modify', methods=['GET','POST'])
def user_modify():
    user = request.form['f_id']
    uname = request.form['uname']
    identify = request.form['identify']
    moodlens = request.form['moodlens']
    profile = request.form['profile']
    propagate = request.form['propagate']
    old_items = db.session.query(UserList).filter(UserList.id==user).all()
    if len(old_items):
        for old_item in old_items:
            pas = old_item.password
            db.session.delete(old_item)
            db.session.commit()
            new_item = UserList(username=uname,password=pas,identify=identify,moodlens=moodlens,profile=profile,propagate=propagate)
            db.session.add(new_item)
            db.session.commit()
        return json.dumps('Right')
    else:        
        return json.dumps('Wrong')

@mod.route('/usertopic_de', methods=['GET','POST'])
def usertopic_de():#定制话题删除
    result = 'Right'
    user_id = request.form['f_id']
    status = request.form['status']
    local = int(time.time())
    old_items = db.session.query(Topics).filter(Topics.id==user_id).all()
    if len(old_items):
        for old_item in old_items:
            topic = old_item.topic.encode('utf-8')
            db_date = old_item.db_date
            db.session.delete(old_item)
            db.session.commit()
            new_item = Topics(user='admin',topic=topic,iscustom=False,expire_date=local,db_date=db_date,status=status)
            db.session.add(new_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route('/usertopic_new', methods=['GET','POST'])
def usertopic_new():#添加定制话题
    result = 'Right'
    new_field = request.form['topic']
    se_weight = request.form['se_weight']
    s = time.mktime(time.strptime(se_weight, '%Y-%m-%d'))
    s = int(s)
    local = int(time.time())
    topics = db.session.query(Topics).filter((Topics.iscustom==True)&(Topics.expire_date>=local)).all()
    if len(topics)>10:
        return json.dumps('outindex')
    old_items = db.session.query(Topics).filter(Topics.topic==new_field).all()
    if len(old_items):
        for old_item in old_items:
            if old_item.iscustom == True:
                result = 'Wrong'
            else:
                topic = old_item.topic.encode('utf-8')
                db.session.delete(old_item)
                db.session.commit()
                new_item = Topics(user='admin',topic=topic,iscustom=True,expire_date=s,db_date=local,status=-1)
                db.session.add(new_item)
                db.session.commit()
    else:
	new_item = Topics(user='admin',topic=new_field,iscustom=True,expire_date=s,db_date=local,status=-1)
	db.session.add(new_item)
	db.session.commit()
    return json.dumps(result)

@mod.route('/usertopic_modify', methods=['GET','POST'])
def usertopic_modify():#修改定制话题过期时间
    user = request.form['f_id']
    new_time = request.form['time']
    s = time.mktime(time.strptime(new_time, '%Y-%m-%d'))
    s = int(s)
    old_items = db.session.query(Topics).filter(Topics.id==user).all()
    if len(old_items):
        for old_item in old_items:
            topic = old_item.topic.encode('utf-8')
            local = old_item.db_date
            db.session.delete(old_item)
            db.session.commit()
            new_item = Topics(user='admin',topic=topic,iscustom=True,expire_date=s,db_date=local,status=-1)
            db.session.add(new_item)
            db.session.commit()
        return json.dumps('Right')
    else:
        return json.dumps('Wrong')

@mod.route('/history_de', methods=['GET','POST'])
def history_de():#删除历史话题
    result = 'Right'
    user_id = request.form['f_id']
    old_items = db.session.query(TopicStatus).filter(TopicStatus.id==user_id).all()
    if len(old_items):
        for old_item in old_items:
            topic = old_item.topic.encode('utf-8')
            start = old_item.start
            end = old_item.end
            local = old_item.db_date
            db.session.delete(old_item)
            db.session.commit()
            new_item = TopicStatus(module='Sentiment',status=-2,topic=topic,start=start,end=end,range=900,db_date=local)
            db.session.add(new_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route('/history_new', methods=['GET','POST'])
def history_new():#添加历史话题
    result = 'Right'
    new_field = request.form['topic']
    start = request.form['start']
    end = request.form['end']
    start_time = time.mktime(time.strptime(start, '%Y-%m-%d'))
    start_time = int(start_time)
    end_time = time.mktime(time.strptime(end, '%Y-%m-%d'))
    end_time = int(end_time)
    local = int(time.time())
    topics = db.session.query(TopicStatus).filter(TopicStatus.status==-1).all()
    if len(topics)>10:
        return json.dumps('outindex')
    old_items = db.session.query(TopicStatus).filter(TopicStatus.topic==new_field).all()
    if len(old_items):
        for old_item in old_items:
            if old_item.status == -2:
                topic = old_item.topic.encode('utf-8')
                db.session.delete(old_item)
                db.session.commit()
                new_item = TopicStatus(module='Sentiment',status=-1,topic=topic,start=start_time,end=end_time,range=900,db_date=local)
                db.session.add(new_item)
                db.session.commit()                
            else:
                result = 'Wrong'
    else:
	new_item = TopicStatus(module='Sentiment',status=-1,topic=new_field,start=start_time,end=end_time,range=900,db_date=local)
        db.session.add(new_item)
        db.session.commit()
    return json.dumps(result)

@mod.route('/check_log', methods=['GET','POST'])
def check_log():
    filename = request.form['file']
    top = request.form['top']

    if filename == '_sentiment_redis2mysql':
        path = '/home/ubuntu12/yuanshi/project/weibo/cron/moodlens/logs/'
    elif filename == 'identify':
        path = '/home/ubuntu12/yuanshi/project/weibo/cron/moodlens/logs/'
    elif filename == 'identify_burst':
        path = '/home/ubuntu12/yuanshi/project/weibo/cron/moodlens/logs/'
    elif filename == 'xapian_zmq_work':
        path = '/home/ubuntu12/linhao/xapian_weibo/zmq_workspace/'
    else:
        path = '/home/ubuntu12/yuanshi/project/weibo/cron/moodlens/logs/'

    logs = read_log(str(path),str(filename),top)

    if len(logs) == 0:
        return json.dumps('Right')

    return json.dumps(logs)
    









    
    
