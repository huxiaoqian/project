# -*- coding: utf-8 -*-

from flask import Blueprint, render_template

mod = Blueprint('index', __name__, url_prefix='')

@mod.route('/')
def index():
    return render_template('root/index.html', active='home')

@mod.route('/help/')
def help():
    return render_template('root/help.html', active='help')

@mod.route('/about/')
def contactxs():
    return render_template('root/about.html', active='about')
