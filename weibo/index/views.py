# -*- coding: utf-8 -*-

from flask import Blueprint, render_template

mod = Blueprint('index', __name__, url_prefix='')

@mod.route('/')
def index():
    return render_template('index/index.html')

@mod.route('/help/')
def help():
    return render_template('index/help.html')

@mod.route('/contact/')
def contactxs():
    return render_template('index/contact.html')
