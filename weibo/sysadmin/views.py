# -*- coding: utf-8 -*-

from flask import Blueprint, url_for, render_template, request, abort, flash, session, redirect
import weibo.model
from weibo.model import *
from weibo.extensions import db
import json
import csv
import os
from xapian_weibo.xapian_backend import XapianSearch
from xapian_config import xapian_search_user

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
        fields = db.session.query(Field).filter().all()
        topics = db.session.query(Topic).filter().all()
        return render_template('admin/para_field.html', fields = fields, topics = topics) 
    else:
        return redirect('/sysadmin/')

@mod.route('/paraset/new/')
def help_new():
    if 'logged_in' in session and session['logged_in']:
        newwords = db.session.query(NewWords).filter().all()
        return render_template('admin/para_new.html', newwords = newwords) 
    else:
        return redirect('/sysadmin/')

@mod.route('/paraset/userlist/')
def help_white():
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
        blacks = db.session.query(BlackList).filter().all()
        return render_template('admin/para_hei.html',blacks = blacks) 
    else:
        return redirect('/sysadmin/')

@mod.route('/paraset/media/')
def help_media():
    if 'logged_in' in session and session['logged_in']:
        medias = db.session.query(IMedia).filter().all()
        return render_template('admin/para_media.html', medias = medias) 
    else:
        return redirect('/sysadmin/')

@mod.route('/paraset/material/')
def help_material():
    if 'logged_in' in session and session['logged_in']:
        materials = db.session.query(M_Weibo).filter().all()
        return render_template('admin/para_ma.html', materials = materials) 
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
    new_item = Field(fieldName=new_field)
    db.session.add(new_item)
    db.session.commit()
    return json.dumps(result)

@mod.route('/add_topic', methods=['GET','POST'])
def add_topic():
    result = 'Right'
    new_field = request.form['topic']
    new_fid = request.form['field_id']
    new_item = Topic(topicName=new_field,fieldId=new_fid)
    db.session.add(new_item)
    db.session.commit()
    return json.dumps(result)

@mod.route('/add_new', methods=['GET','POST'])
def add_new():
    result = 'Right'
    new_field = request.form['topic']
    se_weight = request.form['se_weight']
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
    conditions = db.session.query(Topic).filter(Topic.fieldId==new_id).all()
    if len(conditions):
        result = 'Wrong'
    else:        
        old_items = db.session.query(Field).filter(Field.id==new_id).all()
        for old_item in old_items:
            db.session.delete(old_item)
            db.session.commit()
    return json.dumps(result)

@mod.route('/topic_de', methods=['GET','POST'])
def topic_de():
    result = 'Right'
    new_id = request.form['f_id']
    old_items = db.session.query(Topic).filter(Topic.id==new_id).all()
    for old_item in old_items:
        db.session.delete(old_item)
        db.session.commit()
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
    n = 0
    st = ''
    wid = 'id'
    wna = 'na'
    wwe = 'we'
    for i in range(0,len(f_name)):
        
        if f_name[i] == ',':
            if n==0:
                wid = st
                n = n + 1
            else:
                wna = st
                n = n + 1
            st = ''
        elif f_name[i]=='\n':
            new_item = NewWords(id=wid,wordsName=wna.encode('utf-8'),seWeight=wwe)
            db.session.add(new_item)
            db.session.commit()
            n = 0
            i = i + 1
            st = ''
        else:
            st = st + f_name[i]
            if n == 2:
                wwe = st
                i = i + 1
                n = 0

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
    newwords = db.session.query(NewWords).filter((NewWords.id>startoffset)&(NewWords.id<=endoffset)).all()
    news=[]
    for newword in newwords:
        if newword:
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

@mod.route('/t_rank/')
def t_rank():
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
    newwords = db.session.query(Topic).filter().all()
    news=[]
    n = 0
    for newword in newwords:
        if newword:
            n = n + 1
            if n > startoffset:
                if n > endoffset:
                    break 
                news.append({'id':newword.id,'topicName':newword.topicName.encode('utf-8'),'fieldId':newword.fieldId})
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
    old_items = db.session.query(UserList).filter(UserList.id==user).all()
    if len(old_items):
        return json.dumps('Wrong')
    else:
        new_item = UserList(id=user,password=pas,identify=identify,moodlens=moodlens,profile=profile,propagate=propagate)
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
            new_item = UserList(id=user,password=pas,identify=identify,moodlens=moodlens,profile=profile,propagate=propagate)
            db.session.add(new_item)
            db.session.commit()
        return json.dumps('Right')
    else:        
        return json.dumps('Wrong')

