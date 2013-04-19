# -*- coding: utf-8 -*-

from flask import Flask

import model

from extensions import db, admin
from model_view import SQLModelView

from weibo.index.views import mod as indexModule
from weibo.identify.views import mod as identifyModule

def create_app():
    app = Flask(__name__)
    app.config.from_object('config')

    # Create modules
    app.register_blueprint(indexModule)
    app.register_blueprint(identifyModule)

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
