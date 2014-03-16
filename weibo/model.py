# -*- coding: utf-8 -*-

from extensions import db

__all__ = ['Field', 'Topic', \
           'RangeCount', 'Province', 'PersonalLdaWords', 'HotStatus', 'Media', 'Manager', 'NewWords', \
           'UserWeight', 'BlackList', 'IMedia', 'M_Weibo', 'UserList','Topic_Search', 'SentimentCount', \
           'SentimentKeywords', 'TopWeibos', 'Domain', 'SentimentDomainCount', \
           'SentimentDomainKeywords', 'SentimentDomainTopWeibos', 'SentimentTopicCount', \
           'SentimentTopicKeywords', 'SentimentTopicTopWeibos', 'Topics', 'DomainUser', \
           'SentimentRtTopicCount', 'SentimentRtTopicKeywords', 'SentimentRtTopicTopWeibos', \
           'TopicStatus', 'WholeIdentification', 'AreaIdentification', 'BurstIdentification', \
           'TopicIdentification', 'KnowledgeList', 'PropagateTopic', 'PropagateTrend', \
           'PropagateSpatial', 'PropagateUser', 'PropagateWeibo', 'ProfileDomainTopic', \
           'ProfileDomainBasic', 'ProfileDomainWeiboCount', 'ProfilePersonBasic', \
           'ProfilePersonFriends', 'ProfilePersonTopic', 'ProfilePersonWeiboCount', 'TopicGexf', \
           'WeiboStatus', 'PropagateSingle', 'PropagateTrendSingle', 'PropagateSpatialSingle', 'PropagateUserSingle', 'PropagateWeiboSingle',\
           'PropagateSinglePart', 'PropagateTrendSinglePart', 'PropagateSpatialSinglePart', 'PropagateUserSinglePart', 'PropagateWeiboSinglePart']



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

class AreaIdentification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    topicId = db.Column(db.Integer)
    rank = db.Column(db.Integer)
    userId = db.Column(db.BigInteger(11, unsigned=True))
    followersCount = db.Column(db.BigInteger(20, unsigned=True))
    activeCount = db.Column(db.BigInteger(20, unsigned=True))
    importantCount = db.Column(db.BigInteger(20, unsigned=True))
    identifyDate = db.Column(db.Date)
    identifyWindow = db.Column(db.Integer, default=1)
    identifyMethod = db.Column(db.String(20), default='followers')

    def __init__(self, topicId, rank, userId, followersCount, activeCount, importantCount, identifyDate, identifyWindow, identifyMethod):
        self.topicId = topicId
        self.rank = rank
        self.userId = userId
        self.followersCount = followersCount
        self.activeCount = activeCount
        self.importantCount = importantCount
        self.identifyDate = identifyDate
        self.identifyWindow = identifyWindow
        self.identifyMethod = identifyMethod

class BurstIdentification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    rank = db.Column(db.Integer)
    userId = db.Column(db.BigInteger(11, unsigned=True))
    followersCount = db.Column(db.BigInteger(20, unsigned=True))
    activeCount = db.Column(db.BigInteger(20, unsigned=True))
    importantCount = db.Column(db.BigInteger(20, unsigned=True))
    activeDiff = db.Column(db.BigInteger(20))
    importantDiff = db.Column(db.BigInteger(20))
    identifyDate = db.Column(db.Date)
    identifyWindow = db.Column(db.Integer, default=1)
    identifyMethod = db.Column(db.String(20), default='active')

    def __init__(self, rank, userId, followersCount, activeCount, importantCount, activeDiff, importantDiff, identifyDate, identifyWindow, identifyMethod):
        self.rank = rank
        self.userId = userId
        self.followersCount = followersCount
        self.activeCount = activeCount
        self.importantCount = importantCount
        self.activeDiff = activeDiff
        self.importantDiff = importantDiff
        self.identifyDate = identifyDate
        self.identifyWindow = identifyWindow
        self.identifyMethod = identifyMethod

class TopicIdentification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    topic = db.Column(db.String(20))
    rank = db.Column(db.Integer)
    userId = db.Column(db.BigInteger(11, unsigned=True))
    identifyDate = db.Column(db.Date)
    identifyWindow = db.Column(db.Integer, default=1)
    identifyMethod = db.Column(db.String(20), default='pagerank')

    def __init__(self, topic, rank, userId, identifyDate, identifyWindow, identifyMethod):
        self.topic = topic
        self.rank = rank
        self.userId = userId
        self.identifyDate = identifyDate
        self.identifyWindow = identifyWindow
        self.identifyMethod = identifyMethod 

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

class KnowledgeList(db.Model):
    id = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)
    kID = db.Column(db.BigInteger(11, unsigned=True), unique=True)
    kName = db.Column(db.String(30), unique=True)
    domain = db.Column(db.String(30))#人员类别

    @classmethod
    def _name(cls):
        return u'知识库'

    def __repr__(self):
        return self.kName

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
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    id = db.Column(db.Integer, primary_key=True)
    domain = db.Column(db.Integer)
    range = db.Column(db.BigInteger(10, unsigned=True))
    ts = db.Column(db.BigInteger(10, unsigned=True))
    sentiment = db.Column(db.Integer(1, unsigned=True))
    count = db.Column(db.BigInteger(20, unsigned=True))

    def __init__(self, domain, range, ts, sentiment, count):
        self.domain = domain
        self.range = range
        self.ts = ts
        self.sentiment = sentiment
        self.count = count

class SentimentDomainKeywords(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    domain = db.Column(db.Integer, primary_key=True)
    range = db.Column(db.BigInteger(10, unsigned=True))
    limit = db.Column(db.BigInteger(10, unsigned=True))
    ts = db.Column(db.BigInteger(10, unsigned=True))
    sentiment = db.Column(db.Integer(1, unsigned=True))
    kcount = db.Column(db.Text)

    def __init__(self, domain, range, limit, ts, sentiment, kcount):
        self.domain = domain
        self.range = range
        self.limit = limit
        self.ts = ts
        self.sentiment = sentiment
        self.kcount = kcount

class SentimentDomainTopWeibos(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    domain = db.Column(db.Integer, primary_key=True)
    range = db.Column(db.BigInteger(10, unsigned=True))
    limit = db.Column(db.BigInteger(10, unsigned=True))
    ts = db.Column(db.BigInteger(10, unsigned=True))
    sentiment = db.Column(db.Integer(1, unsigned=True))
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
    db_date = db.Column(db.BigInteger(10, unsigned=True))#入库时间
    status = db.Column(db.Integer)#0:完全删除 1:过期不删除 -1:未删除

    def __init__(self, user, topic, iscustom, expire_date, db_date, status):
        self.user = user
        self.topic = topic
        self.iscustom = iscustom
        self.expire_date = expire_date
        self.db_date = db_date
        self.status = status

class DomainUser(db.Model):
    userId = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)
    domains = db.Column(db.Text)
    updateTime = db.Column(db.String(10))

    def __init__(self, userid, domains, updatetimestr):
        self.userId = userid
        self.domains = domains
        self.updateTime = updatetimestr

class TopicStatus(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    module = db.Column(db.String(10))# 显示是哪个模块
    status = db.Column(db.Integer)# 1: completed 0: computing, -1: not_start -2:删除
    topic = db.Column(db.Text)
    start = db.Column(db.BigInteger(10, unsigned=True))#起始时间
    end = db.Column(db.BigInteger(10, unsigned=True))#终止时间
    range = db.Column(db.BigInteger(10, unsigned=True))#统计单元
    db_date = db.Column(db.BigInteger(10, unsigned=True))#入库时间

    def __init__(self, module, status, topic, start, end, range, db_date):
        self.module = module
        self.status = status
        self.topic = topic
        self.start = start
        self.end = end
        self.range = range
        self.db_date = db_date

class WeiboStatus(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    module = db.Column(db.String(10))# 显示是哪个模块
    status = db.Column(db.Integer)# 1: completed 0: computing, -1: not_start -2:删除
    mid = db.Column(db.String(30))
    postDate = db.Column(db.DateTime)#发布时间
    db_date = db.Column(db.BigInteger(10, unsigned=True))#入库时间

    def __init__(self, module, status, mid, postDate, db_date):
        self.module = module
        self.status = status
        self.mid = mid
        self.postDate = postDate
        self.db_date = db_date

class SentimentRtTopicCount(db.Model):
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

class SentimentRtTopicKeywords(db.Model):
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

class SentimentRtTopicTopWeibos(db.Model):
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

class PropagateTopic(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    topic_name = db.Column(db.String(20))
    image_url = db.Column(db.String(50))
    start = db.Column(db.Date)
    end = db.Column(db.Date)
    raise_user = db.Column(db.String(20))
    t_weibo = db.Column(db.BigInteger(10, unsigned=True))
    o_weibo = db.Column(db.BigInteger(10, unsigned=True))
    raise_time = db.Column(db.Date)
    persistent = db.Column(db.Float)
    sudden = db.Column(db.Float)
    coverage = db.Column(db.Float)
    media = db.Column(db.Float)
    leader = db.Column(db.Float)

    def __init__(self, topic_name, image_url, start, end, raise_user, t_weibo, o_weibo, raise_time, persistent, sudden, coverage, media, leader):
        self.topic_name = topic_name 
        self.image_url = image_url
        self.start = start
        self.end = end
        self.raise_user = raise_user
        self.t_weibo = t_weibo
        self.o_weibo = o_weibo
        self.raise_time = raise_time
        self.persistent = persistent
        self.sudden = sudden
        self.coverage = coverage
        self.media = media
        self.leader = leader

class PropagateTrend(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    topic_id = db.Column(db.Integer)
    date = db.Column(db.Date)
    count = db.Column(db.BigInteger(10, unsigned=True))

    def __init__(self, topic_id, date, count):
        self.topic_id = topic_id 
        self.date = date
        self.count = count

class PropagateSpatial(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    topic_id = db.Column(db.Integer)
    city = db.Column(db.String(20))
    count = db.Column(db.BigInteger(10, unsigned=True))

    def __init__(self, topic_id, city, count):
        self.topic_id = topic_id 
        self.city = city
        self.count = count

class PropagateUser(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    topic_id = db.Column(db.Integer)
    user = db.Column(db.String(20))
    user_name = db.Column(db.String(50))
    location = db.Column(db.String(50))
    follower = db.Column(db.Integer)
    friend = db.Column(db.Integer)
    status = db.Column(db.Integer)
    description = db.Column(db.Text)
    image_url = db.Column(db.String(50))

    def __init__(self, topic_id, user, user_name, location, follower, friend, status, description,image_url):
        self.topic_id = topic_id 
        self.user = user
        self.user_name = user_name
        self.location = location
        self.follower = follower
        self.friend = friend
        self.status = status
        self.description = description
        self.image_url = image_url

class PropagateWeibo(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    topic_id = db.Column(db.Integer)
    mid = db.Column(db.String(30))
    image_url = db.Column(db.String(50))
    text = db.Column(db.Text)
    sourcePlatform = db.Column(db.String(20))
    postDate = db.Column(db.DateTime)
    uid = db.Column(db.String(20))
    user_name = db.Column(db.String(20))
    repostsCount = db.Column(db.Integer)
    commentsCount = db.Column(db.Integer)
    attitudesCount = db.Column(db.Integer)

    def __init__(self, topic_id, mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount):
        self.topic_id = topic_id 
        self.mid = mid
        self.image_url = image_url
        self.text = text
        self.sourcePlatform = sourcePlatform
        self.postDate = postDate
        self.uid = uid
        self.user_name = user_name
        self.repostsCount = repostsCount
        self.commentsCount = commentsCount
        self.attitudesCount = attitudesCount

class PropagateSingle(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    mid = db.Column(db.String(30))
    image_url = db.Column(db.String(50))
    text = db.Column(db.Text)
    sourcePlatform = db.Column(db.String(20))
    postDate = db.Column(db.DateTime)
    uid = db.Column(db.String(20))
    user_name = db.Column(db.String(20))
    repostsCount = db.Column(db.Integer)
    commentsCount = db.Column(db.Integer)
    attitudesCount = db.Column(db.Integer)
    persistent = db.Column(db.Float)
    sudden = db.Column(db.Float)
    coverage = db.Column(db.Float)
    media = db.Column(db.Float)
    leader = db.Column(db.Float)

    def __init__(self, mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount, persistent, sudden, coverage, media, leader):
        self.mid = mid
        self.image_url = image_url
        self.text = text
        self.sourcePlatform = sourcePlatform
        self.postDate = postDate
        self.uid = uid
        self.user_name = user_name
        self.repostsCount = repostsCount
        self.commentsCount = commentsCount
        self.attitudesCount = attitudesCount
        self.persistent = persistent
        self.sudden = sudden
        self.coverage = coverage
        self.media = media
        self.leader = leader

class PropagateTrendSingle(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    mid = db.Column(db.String(30))
    date = db.Column(db.Date)
    count = db.Column(db.BigInteger(10, unsigned=True))

    def __init__(self, mid, date, count):
        self.mid = mid 
        self.date = date
        self.count = count

class PropagateSpatialSingle(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    mid = db.Column(db.String(30))
    city = db.Column(db.String(20))
    count = db.Column(db.BigInteger(10, unsigned=True))

    def __init__(self, mid, city, count):
        self.mid = mid 
        self.city = city
        self.count = count

class PropagateUserSingle(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    mid = db.Column(db.String(30))
    user = db.Column(db.String(20))
    user_name = db.Column(db.String(50))
    location = db.Column(db.String(50))
    follower = db.Column(db.Integer)
    friend = db.Column(db.Integer)
    status = db.Column(db.Integer)
    description = db.Column(db.Text)
    image_url = db.Column(db.String(50))

    def __init__(self, mid, user, user_name, location, follower, friend, status, description,image_url):
        self.mid = mid 
        self.user = user
        self.user_name = user_name
        self.location = location
        self.follower = follower
        self.friend = friend
        self.status = status
        self.description = description
        self.image_url = image_url

class PropagateWeiboSingle(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ori_mid = db.Column(db.String(30))
    mid = db.Column(db.String(30))
    image_url = db.Column(db.String(50))
    text = db.Column(db.Text)
    sourcePlatform = db.Column(db.String(20))
    postDate = db.Column(db.DateTime)
    uid = db.Column(db.String(20))
    user_name = db.Column(db.String(20))
    repostsCount = db.Column(db.Integer)
    commentsCount = db.Column(db.Integer)
    attitudesCount = db.Column(db.Integer)

    def __init__(self, ori_mid, mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount):
        self.ori_mid = ori_mid 
        self.mid = mid
        self.image_url = image_url
        self.text = text
        self.sourcePlatform = sourcePlatform
        self.postDate = postDate
        self.uid = uid
        self.user_name = user_name
        self.repostsCount = repostsCount
        self.commentsCount = commentsCount
        self.attitudesCount = attitudesCount

class PropagateSinglePart(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    mid = db.Column(db.String(30))
    image_url = db.Column(db.String(50))
    text = db.Column(db.Text)
    sourcePlatform = db.Column(db.String(20))
    postDate = db.Column(db.DateTime)
    uid = db.Column(db.String(20))
    user_name = db.Column(db.String(20))
    repostsCount = db.Column(db.Integer)
    commentsCount = db.Column(db.Integer)
    attitudesCount = db.Column(db.Integer)
    persistent = db.Column(db.Float)
    sudden = db.Column(db.Float)
    coverage = db.Column(db.Float)
    media = db.Column(db.Float)
    leader = db.Column(db.Float)

    def __init__(self, mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount, persistent, sudden, coverage, media, leader):
        self.mid = mid
        self.image_url = image_url
        self.text = text
        self.sourcePlatform = sourcePlatform
        self.postDate = postDate
        self.uid = uid
        self.user_name = user_name
        self.repostsCount = repostsCount
        self.commentsCount = commentsCount
        self.attitudesCount = attitudesCount
        self.persistent = persistent
        self.sudden = sudden
        self.coverage = coverage
        self.media = media
        self.leader = leader

class PropagateTrendSinglePart(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    mid = db.Column(db.String(30))
    date = db.Column(db.Date)
    count = db.Column(db.BigInteger(10, unsigned=True))

    def __init__(self, mid, date, count):
        self.mid = mid 
        self.date = date
        self.count = count

class PropagateSpatialSinglePart(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    mid = db.Column(db.String(30))
    city = db.Column(db.String(20))
    count = db.Column(db.BigInteger(10, unsigned=True))

    def __init__(self, mid, city, count):
        self.mid = mid 
        self.city = city
        self.count = count

class PropagateUserSinglePart(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    mid = db.Column(db.String(30))
    user = db.Column(db.String(20))
    user_name = db.Column(db.String(50))
    location = db.Column(db.String(50))
    follower = db.Column(db.Integer)
    friend = db.Column(db.Integer)
    status = db.Column(db.Integer)
    description = db.Column(db.Text)
    image_url = db.Column(db.String(50))

    def __init__(self, mid, user, user_name, location, follower, friend, status, description,image_url):
        self.mid = mid 
        self.user = user
        self.user_name = user_name
        self.location = location
        self.follower = follower
        self.friend = friend
        self.status = status
        self.description = description
        self.image_url = image_url

class PropagateWeiboSinglePart(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ori_mid = db.Column(db.String(30))
    mid = db.Column(db.String(30))
    image_url = db.Column(db.String(50))
    text = db.Column(db.Text)
    sourcePlatform = db.Column(db.String(20))
    postDate = db.Column(db.DateTime)
    uid = db.Column(db.String(20))
    user_name = db.Column(db.String(20))
    repostsCount = db.Column(db.Integer)
    commentsCount = db.Column(db.Integer)
    attitudesCount = db.Column(db.Integer)

    def __init__(self, ori_mid, mid, image_url, text, sourcePlatform, postDate, uid, user_name, repostsCount, commentsCount, attitudesCount):
        self.ori_mid = ori_mid 
        self.mid = mid
        self.image_url = image_url
        self.text = text
        self.sourcePlatform = sourcePlatform
        self.postDate = postDate
        self.uid = uid
        self.user_name = user_name
        self.repostsCount = repostsCount
        self.commentsCount = commentsCount
        self.attitudesCount = attitudesCount

class TopicGexf(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    topic = db.Column(db.String(20))
    identifyDate = db.Column(db.Date)
    identifyWindow = db.Column(db.Integer, default=1)
    identifyGexf = db.Column(db.Text)

    def __init__(self, topic, identifyDate, identifyWindow, identifyGexf):
        self.topic = topic
        self.identifyDate = identifyDate
        self.identifyWindow = identifyWindow
        self.identifyGexf = identifyGexf

class ProfileDomainTopic(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    date = db.Column(db.Date)
    domain = db.Column(db.Integer)
    keywordLen = db.Column(db.Integer, default=50)
    keyword = db.Column(db.Text)

    def __init__(self, domain, keyword, date='2014-01-11', keywordLen=50):
        self.date = date
        self.domain = domain
        self.keywordLen = keywordLen
        self.keyword = keyword

class ProfileDomainBasic(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    date = db.Column(db.Date)
    domain = db.Column(db.Integer)
    provinceCount = db.Column(db.Text)
    verifiedCount = db.Column(db.Integer)
    unverifiedCount = db.Column(db.Integer)

    def __init__(self, domain, provinceCount, verifiedCount, unverifiedCount, date='2014-01-11'):
        self.date = date
        self.domain = domain
        self.provinceCount = provinceCount
        self.verifiedCount = verifiedCount
        self.unverifiedCount = unverifiedCount

class ProfileDomainWeiboCount(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    date = db.Column(db.Date)
    domain = db.Column(db.Integer)
    repostsCount = db.Column(db.BigInteger(20, unsigned=True))
    originalCount = db.Column(db.BigInteger(20, unsigned=True))
    activeCount = db.Column(db.BigInteger(20, unsigned=True))
    importantCount = db.Column(db.BigInteger(20, unsigned=True))

    def __init__(self, domain, repostsCount, originalCount, activeCount, importantCount, date='2014-01-11'):
        self.date = date
        self.domain = domain
        self.repostsCount = repostsCount
        self.originalCount = originalCount
        self.activeCount = activeCount
        self.importantCount = importantCount

class ProfilePersonBasic(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    userId = db.Column(db.BigInteger(11, unsigned=True))
    province = db.Column(db.Integer)
    city = db.Column(db.Integer)
    verified = db.Column(db.Boolean)
    name = db.Column(db.String(20))
    gender = db.Column(db.String(2))
    profileImageUrl = db.Column(db.String(100))
    verifiedType = db.Column(db.Integer)
    friendsCount = db.Column(db.BigInteger(20, unsigned=True))
    followersCount = db.Column(db.BigInteger(20, unsigned=True))
    statuseCount = db.Column(db.BigInteger(20, unsigned=True))    
    location = db.Column(db.String(20))
    description = db.Column(db.Text)
    created_at = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    date = db.Column(db.Date)

    def __init__(self, userId, province, city, verified, name, gender, profileImageUrl, verifiedType, friendsCount, followersCount, statusesCount, location, description, created_at, date='2014-01-11'):
        self.userId = userId
        self.province = province
        self.city = city
        self.verified = verified
        self.name = name
        self.gender = gender
        self.profileImageUrl = profileImageUrl
        self.verifiedType = verifiedType
        self.friendsCount = friendsCount
        self.followersCount = followersCount
        self.statuseCount = statusesCount
        self.location = location
        self.description = description
        self.created_at = created_at
        self.date = date

class ProfilePersonWeiboCount(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    userId = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)
    prod = db.Column(db.Boolean, default=False)
    endDate = db.Column(db.Date)
    windowLen = db.Column(db.Integer, default=30)
    activeSeries = db.Column(db.Text)
    importantSeries = db.Column(db.Text)
    repostsSeries = db.Column(db.Text) # 用户转发微博数序列
    originalSeries = db.Column(db.Text) # 用户原创微博数序列
    emoticonSeries = db.Column(db.Text) # 用户带情感微博数序列

    def __init__(self, userId, activeSeries, importantSeries, repostsSeries, originalSeries, emoticonSeries, endDate, windowLen=30, prod=False):
        self.userId = userId
        self.prod = prod
        self.endDate = endDate
        self.windowLen = windowLen
        self.activeSeries = activeSeries
        self.importantSeries = importantSeries
        self.repostsSeries = repostsSeries
        self.originalSeries = originalSeries
        self.emoticonSeries = emoticonSeries

class ProfilePersonTopic(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    userId = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)
    prod = db.Column(db.Boolean, default=False)
    endDate = db.Column(db.Date)
    windowLen = db.Column(db.Integer, default=7)
    keywordLen = db.Column(db.Integer, default=50)
    keywordSeries = db.Column(db.Text)

    def __init__(self, userId, keywordSeries, endDate, keywordLen=50, windowLen=7, prod=False):
        self.userId = userId
        self.prod = prod
        self.endDate = endDate
        self.windowLen = windowLen
        self.keywordLen = keywordLen
        self.keywordSeries = keywordSeries

class ProfilePersonFriends(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    userId = db.Column(db.BigInteger(11, unsigned=True), primary_key=True)
    relation = db.Column(db.String(10), default='nobody') # 'friends', 'followers', 'nobody'
    prod = db.Column(db.Boolean, default=False)
    endDate = db.Column(db.Date)
    windowLen = db.Column(db.Integer, default=7)
    directInteractSeries = db.Column(db.Text)
    retweetedInteractSeries = db.Column(db.Text)

    def __init__(self, userId, directInteractSeries, retweetedInteractSeries, endDate, windowLen=7, prod=False, relation='nobody'):
        self.userId = userId
        self.relation = relation
        self.prod = prod
        self.endDate = endDate
        self.windowLen = windowLen
        self.directInteractSeries = directInteractSeries
        self.retweetedInteractSeries = retweetedInteractSeries
