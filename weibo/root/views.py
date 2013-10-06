# -*- coding: utf-8 -*-

from flask import Blueprint, url_for, render_template, request, abort, flash, session, redirect
import weibo.model
from weibo.model import *
from weibo.extensions import db
import json

mod = Blueprint('index', __name__, url_prefix='')

@mod.route('/')
def loading():
    return render_template('root/loading.html', active='loading')

@mod.route('/login/')
def index():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            return render_template('root/index.html', active='home',identy='1',moodlens='1',profile='1',propagate='1')
        else:
            pas = db.session.query(UserList).filter(UserList.id==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.identify
                    moodlens = pa.moodlens
                    profile = pa.profile
                    propagate = pa.propagate
            return render_template('root/index.html', active='home',identy=str(identy),moodlens=str(moodlens),profile=str(profile),propagate=str(propagate))
    else:
        return redirect('/')
    

@mod.route('/help/')
def help():
    return render_template('root/help.html', active='help')

@mod.route('/about/')
def contacts():
    return render_template('root/about.html', active='about')

@mod.route('/user_in', methods=['GET','POST'])
def user_in():
    result = None
    if 'logged_in' in session and session['logged_in']:
        session.pop('user', None)
        session.pop('logged_in', None)
    if request.method == 'POST':
        identy = request.form['identy']
        
        if identy == '1':
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
        else:
            pas = db.session.query(UserList).filter(UserList.id==request.form['user']).all()
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

    return render_template('root/loading.html', result = result)

@mod.route('/logout/')
def logout():
    session.pop('user', None)
    session.pop('logged_in', None)
    flash('退出成功！')
    return redirect('/')


