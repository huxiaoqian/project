# Flask 指南

**作者: linhao.** 

## 第一节 安装指南

### 1.1 Flask 安装
[flask installation](http://flask.pocoo.org/docs/installation/)

### 1.2 Flask-SQLAlchemy 安装
install Flask-SQLAlchemy from pip
```
pip install flask-sqlalchemy
```
[more here about the Flask-SQLAlchemy package](http://packages.python.org/Flask-SQLAlchemy/)

## 第二节 概述

### 2.1 文件夹组织结构
Here are the folder structures:
```
/config.py
/README.md
/run.py
/weibo.fcgi
/weibo/
/conf/
/restful_api/
/sql/
```

文件及目录功能解释如下：

* config.py保存系统的全局变量；
* run.py为flask自制web程序的启动入口；
* weibo.fcgi为nginx启动程序；
* weibo文件夹为项目各模块实现的代码库；
* restful_api文件夹将系统中与xapian检索相关的服务封装成restful api的形式以供调用

### 2.2 web系统运行方式

提供两种模式的启动

第一种是flask框架内置的web服务器，在根目录下启动如下如下代码：
```
python run.py -p 9004
```
其中-p参数指定的是系统运行的端口号

第二中是将web程序部署在Nginx服务器上，在根目录下运行如下代码：
```
./weibo.fcgi
```
更详细配置请参考[REDME.md](https://github.com/linhaobuaa/project/blob/master/README.md)

## 第三节 Flask-SQLAlchemy的使用

3.1 简介

对象关系映射器（Object Relational Mappers，ORM）在过去数年吸引了不少人的目光。主要原因是 ORM 经常会在 Web 应用程序框架中被提起，因为它是快速开发（Rapid Development）栈中的关键组件。Django 和 Ruby on Rails 等 Web 框架采用了设计一个独立栈的方法，将自主开发的 ORM 紧密集成到该框架中。而其他框架，如 Pylons、Turbogears 和 Grok，则采用更加基于组件的架构结合可交换的第三方组件。两种方法都有各自的优势：紧密集成允许非常连贯的体验（如果问题映射到框架），而基于组件的架构则允许最大的设计灵活性。但是，本文的主题并不是 Web 框架；而是 SQLAlchemy。

SQLAlchemy 的一个目标是提供能兼容众多数据库（如 SQLite、MySQL、Postgres、Oracle、MS-SQL、SQLServer 和 Firebird）的企业级持久性模型。SQLAlchemy 正处于积极开发阶段，当前最新的 API 将围绕版本 0.5 设计。请参阅参考资料部分，获取官方 API 文档、教程和 SQLAlchemy 书籍的链接。

3.2 使用经验
首先在项目路径/weibo/model.py中通过类的形式定义数据库表的结构
**/weibo/model.py**
```
# -*- coding: utf-8 -*-

from extensions import db

__all__ = ['WholeIdentification']

class WholeIdentification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    rank = db.Column(db.Integer)
    userId = db.Column(db.BigInteger(11, unsigned=True))
    followersCount = db.Column(db.BigInteger(20, unsigned=True))
    activeCount = db.Column(db.BigInteger(20, unsigned=True))
    importantCount = db.Column(db.BigInteger(20, unsigned=True))
    identifyDate = db.Column(db.Date)
    identifyWindow = db.Column(db.Integer, default=1)
    identifyMethod = db.Column(db.String(20), default='followers')

    def __init__(self, rank, userId, followersCount, activeCount, importantCount, identifyDate, identifyWindow, identifyMethod):
        self.rank = rank
        self.userId = userId
        self.followersCount = followersCount
        self.activeCount = activeCount
        self.importantCount = importantCount
        self.identifyDate = identifyDate
        self.identifyWindow = identifyWindow
        self.identifyMethod = identifyMethod
```

在/weibo/extensions.py中定义db对象
**/weibo/extensions.py**
```
# -*- coding: utf-8 -*-

from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext import admin

db = SQLAlchemy()
admin = admin.Admin(name=u'XXX系统 数据库管理')
```

在/weibo/__init__.py中隐式创建数据库
**/weibo/__init__.py**
```
from extensions import db

def create_app():
    # Create database
    db.init_app(app)
    with app.test_request_context():
        db.create_all()
    
    return app
```

数据库的增删改查
```
  # 查询满足条件的所有item
  items = db.session.query(SentimentTopicKeywords).filter(SentimentTopicKeywords.query==topicname).all()
  for item in items:
    # 删除某条记录
  	db.session.delete(item)
  # 将操作提交
  db.session.commit()
```
```
  item = TopicStatus(module, IN_CALC_STATUS, topic, start, end, range, db_date)
  # 添加记录
  db.session.add(item)
  # 将操作提交
  db.session.commit()
```

## 第四节 Flask中views.py文件的编写

TODO

## 第五节 Flask中blueprint的使用

TODO
