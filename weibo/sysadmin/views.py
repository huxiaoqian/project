# -*- coding: utf-8 -*-

from flask import Blueprint, url_for, render_template, request, abort, flash, session, redirect
import weibo.model
from weibo.model import *
from weibo.extensions import db
import json
import csv
import os

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

@mod.route('/paraset/white/')
def help_white():
    if 'logged_in' in session and session['logged_in']:
        whites = db.session.query(WhiteList).filter().all()
        return render_template('admin/para_white.html', whites = whites) 
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
            new_item = Manager(password=new_pass,managerName=session['user'],managerGender='男',managerAge='30',managerPosition='系统管理员')
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
    new_item = NewWords(wordsName=new_field)
    db.session.add(new_item)
    db.session.commit()
    return json.dumps(result)

@mod.route('/add_white', methods=['GET','POST'])
def add_white():
    result = 'Right'
    new_field = request.form['topic']
    new_item = WhiteList(listName=new_field)
    db.session.add(new_item)
    db.session.commit()
    return json.dumps(result)

@mod.route('/add_black', methods=['GET','POST'])
def add_black():
    result = 'Right'
    new_field = request.form['topic']
    new_names = db.session.query(User).filter(User.id==new_field).all()
    for new_name in new_names:
        new_item = BlackList(blackID=new_field,blackName=new_name.userName)
        db.session.add(new_item)
        db.session.commit()
    return json.dumps(result)

@mod.route('/add_media', methods=['GET','POST'])
def add_media():
    result = 'Right'
    new_field = request.form['topic']
    new_names = db.session.query(User).filter(User.id==new_field).all()
    for new_name in new_names:
        new_item = IMedia(mediaID=new_field,mediaName=new_name.userName)
        db.session.add(new_item)
        db.session.commit()
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
    conditions = db.session.query(AreaUserIdentification).filter(AreaUserIdentification.topicId==new_id).all()
    if len(conditions):
        result = 'Wrong'
    else:
        old_items = db.session.query(Topic).filter(Topic.id==new_id).all()
        for old_item in old_items:
            db.session.delete(old_item)
            db.session.commit()
    return json.dumps(result)

@mod.route('/new_de', methods=['GET','POST'])
def new_de():
    result = 'Right'
    new_id = request.form['f_id']
    old_items = db.session.query(NewWords).filter(NewWords.wordsName==new_id).all()
    if len(old_items):
        for old_item in old_items:
            db.session.delete(old_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route('/white_de', methods=['GET','POST'])
def white_de():
    result = 'Right'
    new_id = request.form['f_id']
    old_items = db.session.query(WhiteList).filter(WhiteList.listName==new_id).all()
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


