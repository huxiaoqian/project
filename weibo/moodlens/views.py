# -*- coding: utf-8 -*-

from flask import Blueprint, render_template

mod = Blueprint('moodlens', __name__, url_prefix='/moodlens')


@mod.route('/')
def index():
    return render_template('moodlens/index.html', active='moodlens')
