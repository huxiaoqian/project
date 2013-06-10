# -*- coding: utf-8 -*-

from flask import Flask

import model

from extensions import db, admin
from model_view import SQLModelView

from weibo.root.views import mod as rootModule
from weibo.identify.views import mod as identifyModule
from weibo.moodlens.views import mod as moodlensModule
from weibo.profile.views import mod as profileModule
from weibo.sysadmin.views import mod as adminModule

def create_app():
    app = Flask(__name__)
    app.config.from_object('config')

    # Create modules
    app.register_blueprint(rootModule)
    app.register_blueprint(identifyModule)
    app.register_blueprint(moodlensModule)
    app.register_blueprint(profileModule)
    app.register_blueprint(adminModule)

    # Create database
    db.init_app(app)
    with app.test_request_context():
        db.create_all()

    # Create admin
    admin.init_app(app)
    for m in model.__all__:
        m = getattr(model, m)
        n = m._name()
        admin.add_view(SQLModelView(m, db.session, name=n))

    return app
