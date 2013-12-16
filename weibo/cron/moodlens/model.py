# -*- coding: utf-8 -*-

from config import db

__all__ = ['SentimentCount', 'SentimentKeywords', 'TopWeibos', 'Domain', \
           'SentimentDomainCount', 'SentimentDomainKeywords', 'SentimentDomainTopWeibos', \
           'SentimentTopicCount', 'SentimentTopicKeywords', 'SentimentTopicTopWeibos', 'Topics']


class SentimentCount(db.Model):
    range = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    ts = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    sentiment = db.Column(db.Integer(1, unsigned=True), primary_key=True)
    count = db.Column(db.BigInteger(20, unsigned=True))

    def __init__(self, range, ts, sentiment, count):
        self.range = range
        self.ts = ts
        self.sentiment = sentiment
        self.count = count

class SentimentKeywords(db.Model):
    range = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    limit = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    ts = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    sentiment = db.Column(db.Integer(1, unsigned=True), primary_key=True)
    kcount = db.Column(db.Text)

    def __init__(self, range, limit, ts, sentiment, kcount):
        self.range = range
        self.limit = limit
        self.ts = ts
        self.sentiment = sentiment
        self.kcount = kcount

class TopWeibos(db.Model):
    range = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    limit = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    ts = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    sentiment = db.Column(db.Integer(1, unsigned=True), primary_key=True)
    weibos = db.Column(db.Text)

    def __init__(self, range, limit, ts, sentiment, weibos):
        self.range = range
        self.limit = limit
        self.ts = ts
        self.sentiment = sentiment
        self.weibos = weibos

class Domain(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    idx = db.Column(db.Integer, unique=True)
    name = db.Column(db.String(20), unique=True)
    zhname = db.Column(db.String(20), unique=True)
    active = db.Column(db.Boolean)

    def __init__(self, idx, name, zhname, active):
        self.idx = idx
        self.name = name
        self.zhname = zhname
        self.active = active

class SentimentDomainCount(db.Model):
    domain = db.Column(db.Integer, primary_key=True)
    range = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    ts = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    sentiment = db.Column(db.Integer(1, unsigned=True), primary_key=True)
    count = db.Column(db.BigInteger(20, unsigned=True))

    def __init__(self, domain, range, ts, sentiment, count):
        self.domain = domain
        self.range = range
        self.ts = ts
        self.sentiment = sentiment
        self.count = count

class SentimentDomainKeywords(db.Model):
    domain = db.Column(db.Integer, primary_key=True)
    range = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    limit = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    ts = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    sentiment = db.Column(db.Integer(1, unsigned=True), primary_key=True)
    kcount = db.Column(db.Text)

    def __init__(self, domain, range, limit, ts, sentiment, kcount):
        self.domain = domain
        self.range = range
        self.limit = limit
        self.ts = ts
        self.sentiment = sentiment
        self.kcount = kcount

class SentimentDomainTopWeibos(db.Model):
    domain = db.Column(db.Integer, primary_key=True)
    range = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    limit = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    ts = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    sentiment = db.Column(db.Integer(1, unsigned=True), primary_key=True)
    weibos = db.Column(db.Text)

    def __init__(self, domain, range, limit, ts, sentiment, weibos):
        self.domain = domain
        self.range = range
        self.limit = limit
        self.ts = ts
        self.sentiment = sentiment
        self.weibos = weibos

class SentimentTopicCount(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    query = db.Column(db.String(20))
    end = db.Column(db.BigInteger(10, unsigned=True))
    range = db.Column(db.BigInteger(10, unsigned=True))
    sentiment = db.Column(db.Integer(1, unsigned=True))
    count = db.Column(db.BigInteger(20, unsigned=True))

    def __init__(self, query, range, end, sentiment, count):
        self.query = query 
        self.range = range
        self.end = end
        self.sentiment = sentiment
        self.count = count

class SentimentTopicKeywords(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    query = db.Column(db.String(20))
    end = db.Column(db.BigInteger(10, unsigned=True))
    range = db.Column(db.BigInteger(10, unsigned=True))
    limit = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    sentiment = db.Column(db.Integer(1, unsigned=True))
    kcount = db.Column(db.Text)

    def __init__(self, query, range, limit, end, sentiment, kcount):
        self.query = query 
        self.range = range
        self.limit = limit
        self.end = end
        self.sentiment = sentiment
        self.kcount = kcount

class SentimentTopicTopWeibos(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    query = db.Column(db.String(20))
    end = db.Column(db.BigInteger(10, unsigned=True))
    range = db.Column(db.BigInteger(10, unsigned=True))
    limit = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    sentiment = db.Column(db.Integer(1, unsigned=True))
    weibos = db.Column(db.Text)

    def __init__(self, query, range, limit, end, sentiment, weibos):
        self.query = query 
        self.range = range
        self.limit = limit
        self.end = end
        self.sentiment = sentiment
        self.weibos = weibos

class Topics(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user = db.Column(db.String(20))
    topic = db.Column(db.Text)
    iscustom = db.Column(db.Boolean)
    expire_date = db.Column(db.BigInteger(10, unsigned=True))

    def __init__(self, user, topic, iscustom, expire_date):
        self.user = user
        self.topic = topic
        self.iscustom = iscustom
        self.expire_date = expire_date