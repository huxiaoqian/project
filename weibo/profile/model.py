# -*- coding: utf-8 -*-

from config import db
import json

__all__ = ['Field', 'Topic', 'User', 'Status', 'RepostRelationship', 'FollowRelationship',
           'UserIdentification', 'RangeCount', 'Province', 'Words', 'PersonalBurstWords',
           'FieldProfile', 'UserField']

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

class User(db.Model):
    id = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)
    userName = db.Column(db.String(30))
    location = db.Column(db.String(20))
    gender = db.Column(db.String(5))
    profileImageUrl = db.Column(db.String(50))
    description = db.Column(db.String(150))
    createdAt = db.Column(db.DateTime)
    verified = db.Column(db.Boolean)
    verifiedType = db.Column(db.Integer)
    statusesCount = db.Column(db.Integer)
    followersCount = db.Column(db.Integer)
    friendsCount = db.Column(db.Integer)
    biFollowersCount = db.Column(db.Integer)

    @classmethod
    def _name(cls):
        return u'博主'

    def __repr__(self):
        return self.userName

    @property
    def serialize(self):
        """Return object data in easily serializeable format"""
        return {
            'id' : self.id,
            'userName' : self.userName,
            'location' : self.location,
            'gender' : self.gender,
            'profileImageUrl' : self.profileImageUrl,
            'description' : self.description,
            'createdAt' : self.createdAt.isoformat(),
            'verified' : self.verified,
            'verifiedType' : self.verifiedType,
            'statusesCount' : self.statusesCount,
            'followersCount' : self.followersCount,
            'friendsCount' : self.friendsCount,
            'biFollowersCount' : self.biFollowersCount
        }

class Status(db.Model):
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

    @classmethod
    def _name(cls):
        return u'微博'

    def __repr__(self):
        return self.id

class RepostRelationship(db.Model):
    id = db.Column(db.BigInteger(20, unsigned=True), primary_key=True)
    fieldId = db.Column(db.Integer, db.ForeignKey('field.id'))
    field = db.relationship('Field', primaryjoin='Field.id==RepostRelationship.fieldId')
    topicId = db.Column(db.Integer, db.ForeignKey('topic.id'))
    topic = db.relationship('Topic', primaryjoin='Topic.id==RepostRelationship.topicId')
    uid =  db.Column(db.BigInteger(11, unsigned=True))
    sourceUid = db.Column(db.BigInteger(11, unsigned=True))
    mid = db.Column(db.BigInteger(20, unsigned=True))
    sourceMid = db.Column(db.BigInteger(20, unsigned=True))
    createdAt = db.Column(db.DateTime)

    @classmethod
    def _name(cls):
        return u'转发关系'

    def __repr__(self):
        return self.id

class FollowRelationship(db.Model):
    uid = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)
    fid = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)

    @classmethod
    def _name(cls):
        return u'关注关系'

    def __repr__(self):
        return '%s->%s' % (self.uid, self.fid)

class UserIdentification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    fieldId = db.Column(db.Integer, db.ForeignKey('field.id'))
    field = db.relationship('Field', primaryjoin='Field.id==UserIdentification.fieldId')
    topicId = db.Column(db.Integer, db.ForeignKey('topic.id'))
    topic = db.relationship('Topic', primaryjoin='Topic.id==UserIdentification.topicId')
    rank = db.Column(db.Integer)
    userId = db.Column(db.BigInteger(11, unsigned=True), db.ForeignKey('user.id'))
    user = db.relationship('User', primaryjoin='User.id==UserIdentification.userId')
    identifyRange = db.Column(db.String(20), default='area')
    identifyDate = db.Column(db.Date)
    identifyWindow = db.Column(db.Integer, default=1)
    identifyMethod = db.Column(db.String(20), default='PageRank')

    @classmethod
    def _name(cls):
        return u'博主识别'

    def __repr__(self):
        return self.id


'''以下是模块3新增的表
'''
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

class Words(db.Model):
    id = db.Column(db.BigInteger(20, unsigned=True), primary_key=True)
    uid = db.Column(db.BigInteger(11, unsigned=True))
    postDate = db.Column(db.DateTime)
    textWord = db.Column(db.String(400))
    retweetedMid = db.Column(db.BigInteger(20, unsigned=True))

    @classmethod
    def _name(cls):
        return u'词语'

    def __repr__(self):
        return self.id

class PersonalBurstWords(db.Model):
    uid = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)
    windowSize = db.Column(db.BigInteger(20, unsigned=True), primary_key=True) 
    startDate = db.Column(db.Date, primary_key=True)
    endDate = db.Column(db.Date, primary_key=True)
    word = db.Column(db.String(20), primary_key=True)
    burst = db.Column(db.Float(10,4))
    freq = db.Column(db.BigInteger(20, unsigned=True))
    
    @classmethod
    def _name(cls):
        return u'突发词'

    def __repr__(self):
        return self.startdate

class FieldProfile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    fieldEnName = db.Column(db.String(20), unique=True)
    fieldZhName = db.Column(db.String(20), unique=True)

    @classmethod
    def _name(cls):
        return u'画像用领域'

    def __repr__(self):
        return self.id

class UserField(db.Model):
    uid = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)
    updateTime = db.Column(db.Date, primary_key=True)
    fieldFirst = db.Column(db.String(20))
    fieldSecond = db.Column(db.String(20))
    
    @classmethod
    def _name(cls):
        return u'领域博主'

    def __repr__(self):
        return self.uid
