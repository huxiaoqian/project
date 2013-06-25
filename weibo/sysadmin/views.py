# -*- coding: utf-8 -*-

from flask import Blueprint, render_template

mod = Blueprint('sysadmin', __name__, url_prefix='/sysadmin')

@mod.route('/')
def index():
    return render_template('admin/index.html')

@mod.route('/paraset/')
def help():
    return render_template('admin/para_setting.html')

@mod.route('/usermanage/')
def contacts():
    return render_template('admin/user_manage.html')
