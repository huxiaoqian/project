# -*- coding: utf-8 -*-

from extensions import db

__all__ = ['Field', 'Topic', 'WholeUserIdentification', 'AreaUserIdentification', 'BurstUserIdentification', 
           'RangeCount', 'Province', 'PersonalLdaWords', 'HotStatus', 'Media', 'Manager', 'NewWords', 
           'UserWeight', 'BlackList', 'IMedia', 'M_Weibo', 'UserList','Topic_Search']


class Field(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    fieldName = db.Column(db.String(20), unique=True)

    @classmethod
    def _name(cls):
        return u'领域'

    def __repr__(self):
        return self.fieldName

class Topic(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    topicName = db.Column(db.String(20), unique=True)
    fieldId = db.Column(db.Integer, db.ForeignKey('field.id'))
    field = db.relationship('Field', primaryjoin='Field.id==Topic.fieldId')

    @classmethod
    def _name(cls):
        return u'子领域, 话题'

    def __repr__(self):
        return self.topicName

class WholeUserIdentification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    rank = db.Column(db.Integer)
    userId = db.Column(db.BigInteger(11, unsigned=True))
    identifyDate = db.Column(db.Date)
    identifyWindow = db.Column(db.Integer, default=1)
    identifyMethod = db.Column(db.String(20), default='followers')

    @classmethod
    def _name(cls):
        return u'全网博主识别'

    def __repr__(self):
        return self.id

class AreaUserIdentification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    topicId = db.Column(db.Integer)
    rank = db.Column(db.Integer)
    userId = db.Column(db.BigInteger(11, unsigned=True))
    identifyDate = db.Column(db.Date)
    identifyWindow = db.Column(db.Integer, default=1)
    identifyMethod = db.Column(db.String(20), default='pagerank')

    @classmethod
    def _name(cls):
        return u'领域博主识别'

    def __repr__(self):
        return self.id

class BurstUserIdentification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    rank = db.Column(db.Integer)
    userId = db.Column(db.BigInteger(11, unsigned=True))
    identifyDate = db.Column(db.Date)
    identifyWindow = db.Column(db.Integer, default=1)
    identifyMethod = db.Column(db.String(20), default='followers')

    @classmethod
    def _name(cls):
        return u'突发博主识别'

    def __repr__(self):
        return self.id

class RangeCount(db.Model):
    index = db.Column(db.Integer, primary_key=True)
    countType = db.Column(db.String(10), primary_key=True)
    upBound = db.Column(db.BigInteger(20, unsigned=True))
    lowBound = db.Column(db.BigInteger(20, unsigned=True))

    @classmethod
    def _name(cls):
        return u'微博或关注或粉丝数量范围'

    def __repr__(self):
        return self.index

class Province(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    province = db.Column(db.String(20), unique=True)

    @classmethod
    def _name(cls):
        return u'省份'

    def __repr__(self):
        return self.id

class PersonalLdaWords(db.Model):
    uid = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)
    windowTimestamp = db.Column(db.BigInteger(20, unsigned=True), primary_key=True) 
    startTimestamp = db.Column(db.BigInteger(20, unsigned=True), primary_key=True)
    word = db.Column(db.String(20000))
    
    @classmethod
    def _name(cls):
        return u'突发词'

    def __repr__(self):
        return self.startTimestamp

class Media(db.Model):
    id = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)
    updateTime = db.Column(db.Date, primary_key=True)
    mediaName = db.Column(db.String(20))
    
    @classmethod
    def _name(cls):
        return u'主流媒体'

    def __repr__(self):
        return self.id

class HotStatus(db.Model):
    id = db.Column(db.BigInteger(20, unsigned=True), primary_key=True)
    text = db.Column(db.String(350))
    geo = db.Column(db.String(50))
    sourcePlatform = db.Column(db.String(20))
    postDate = db.Column(db.DateTime)
    uid = db.Column(db.BigInteger(11, unsigned=True))
    retweetedMid = db.Column(db.BigInteger(20, unsigned=True))
    repostsCount = db.Column(db.Integer)
    commentsCount = db.Column(db.Integer)
    attitudesCount = db.Column(db.Integer)
    updateTime = db.Column(db.Date, primary_key=True)
    
    @classmethod
    def _name(cls):
        return u'热门微博'

    def __repr__(self):
        return self.id

class Manager(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    password = db.Column(db.String(20), unique=True)
    managerName = db.Column(db.String(30))
    managerGender = db.Column(db.String(5))
    managerAge = db.Column(db.Integer)
    managerPosition = db.Column(db.String(30))

    @classmethod
    def _name(cls):
        return u'管理员'

    def __repr__(self):
        return self.managerName

class NewWords(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    wordsName = db.Column(db.String(20), unique=True)
    seWeight = db.Column(db.Integer)

    @classmethod
    def _name(cls):
        return u'新词'

    def __repr__(self):
        return self.wordsName

class UserWeight(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    weightName = db.Column(db.String(20), unique=True)
    weight = db.Column(db.Integer)

    @classmethod
    def _name(cls):
        return u'博主指标权重'

    def __repr__(self):
        return self.weightName

class BlackList(db.Model):
    id = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)
    blackID = db.Column(db.BigInteger(11, unsigned=True), unique=True)
    blackName = db.Column(db.String(30), unique=True)

    @classmethod
    def _name(cls):
        return u'黑名单'

    def __repr__(self):
        return self.blackName

class IMedia(db.Model):
    id = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)
    mediaID = db.Column(db.BigInteger(11, unsigned=True), unique=True)
    mediaName = db.Column(db.String(30), unique=True)

    @classmethod
    def _name(cls):
        return u'重要媒体'

    def __repr__(self):
        return self.mediaName

class M_Weibo(db.Model):
    weibo_id = db.Column(db.BigInteger(20, unsigned=True), primary_key=True)
    text = db.Column(db.String(350))
    repostsCount = db.Column(db.Integer)
    commentsCount = db.Column(db.Integer)
    postDate = db.Column(db.DateTime)
    uid = db.Column(db.BigInteger(11, unsigned=True))

    @classmethod
    def _name(cls):
        return u'微博素材'

    def __repr__(self):
        return self.weibo_id

class UserList(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(20), unique=True)
    password = db.Column(db.String(20))
    identify = db.Column(db.Integer)
    moodlens = db.Column(db.Integer)
    profile = db.Column(db.Integer)
    propagate = db.Column(db.Integer)

    @classmethod
    def _name(cls):
        return u'用户权限列表'

    def __repr__(self):
        return self.id

class Topic_Search(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    uname = db.Column(db.String(20))
    topicName = db.Column(db.String(20), unique=True)

    @classmethod
    def _name(cls):
        return u'用户搜索话题列表'

    def __repr__(self):
        return self.id
