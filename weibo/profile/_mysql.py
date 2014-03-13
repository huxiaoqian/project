# -*- coding: utf-8 -*-

import MySQLdb
from time_utils import ts2HMS
try:
    from weibo.extensions import db
    from weibo.model import ProfilePersonBasic, ProfilePersonWeiboCount
    from weibo.global_config import COBAR_HOST, COBAR_PORT, COBAR_USER
except:
    print 'not in web environment'          

class Person():
    userId = None
    profileImageUrl = None
    name = None
    friendsCount = None
    statuseCount = None
    followersCount = None
    gender = None
    verified = None
    created_at = None
    location = None
    date = None
    verifiedType = None
    description = None
    domain = None

    def __init__(self, person_tuple):
        _id, userId, province, city, verified, name, gender, profileImageUrl, verifiedType, friendsCount, followersCount, statuseCount, location, description, created_at, domain, date = person_tuple
        self.userId = userId
        self.profileImageUrl = profileImageUrl
        self.name = name
        self.friendsCount = friendsCount
        self.statuseCount = statuseCount
        self.followersCount = followersCount
        self.gender = gender
        self.verified = verified
        self.created_at = created_at
        self.location = location
        self.date = date
        self.verifiedType = verifiedType
        self.domain = domain
        self.description = description

    def _to_dict(self):
        person_dict = {}
        person_dict['statuses_count'] = self.statuseCount
        person_dict['followers_count'] = self.followersCount
        person_dict['friends_count'] = self.friendsCount
        person_dict['name'] = self.name
        person_dict['description'] = self.description
        person_dict['profile_image_url'] = self.profileImageUrl
        person_dict['_id'] = self.userId
        person_dict['location'] = self.location
        person_dict['gender'] = u'男' if self.gender == 'm' else u'女'
        person_dict['verifiedType'] = self.verifiedType
        person_dict['verified'] = u'是' if self.verified else u'否'
        person_dict['created_at'] = ts2HMS(self.created_at)
        person_dict['domain'] = self.domain

        return person_dict


def _search_person_basic(userId, sharding=False):
    if sharding:
        person = None
        cobar_conn = MySQLdb.connect(host=COBAR_HOST, user=COBAR_USER, db='cobar_db_weibo', port=COBAR_PORT, charset='utf8')
        cursor = cobar_conn.cursor()
        sql = "SELECT * from profile_person_basic where userId = %d " % int(userId)

        try:
            cursor.execute(sql)
            person = cursor.fetchone()
        except Exception, e:
            print e

        if person:
            person = Person(person)

        cobar_conn.close()        

    else:
        person = ProfilePersonBasic.query.filter_by(userId=userId).first()
    
    if person:
        return 'success', person
    else:
        return 'failed', None


def _search_person_important_active(userId, sharding=False):
    person = ProfilePersonWeiboCount.query.filter_by(userId=userId).first()
    if person:
        return 'success', person
    else:
        return 'failed', None


def _multi_search(query_dict, sharding=True):
    pros = []
    if '$and' in query_dict:
        and_querys = query_dict['$and']
        for q in and_querys:
            if 'statuses_count' in q:
                s_low = q['statuses_count']['$gt']
                s_up = q['statuses_count']['$lt']

            if 'followers_count' in q:
                fol_low = q['followers_count']['$gt']
                fol_up = q['followers_count']['$lt']

            if 'friends_count' in q:
                fri_low = q['friends_count']['$gt']
                fri_up = q['friends_count']['$lt']

            if '$or' in q:
                or_querys = q['$or']
                for _q in or_querys:
                    pro = _q['location']
                    if pro != '':
                        pros.append(pro)        

    if sharding:
        cobar_conn = MySQLdb.connect(host=COBAR_HOST, user=COBAR_USER, db='cobar_db_weibo', port=COBAR_PORT, charset='utf8')
    else:
        cobar_conn = MySQLdb.connect(host='192.168.2.11', user='root', db='weibo', charset='utf8')

    persons = []
    cursor = cobar_conn.cursor()
    location_query = ""

    if len(pros):
        location_query += "and ("
        for iter_count, pro in enumerate(pros):
            if iter_count != len(pros) - 1:
                location_query += "location like '%s' OR " % (pro)
            else:
                location_query += "location like '%s'" % (pro)                
        location_query += ") "

    sql = "SELECT * from profile_person_basic where statuseCount > %d  and statuseCount < %d and followersCount > %d and \
          followersCount < %d and friendsCount > %d and friendsCount < %d " % (s_low, \
          s_up, fol_low, fol_up, fri_low, fri_up) + location_query + "ORDER BY followersCount DESC LIMIT 100"

    try:
        cursor.execute(sql)
        results = cursor.fetchall()
    except:
        return persons

    for person in results:
        person_dict = {}
        _id, userId, province, city, verified, name, gender, profileImageUrl, verifiedType, friendsCount, followersCount, statuseCount, location, description, created_at, domain, date = person
        person_dict['statuses_count'] = statuseCount
        person_dict['followers_count'] = followersCount
        person_dict['friends_count'] = friendsCount
        person_dict['name'] = name
        person_dict['description'] = description
        person_dict['profile_image_url'] = profileImageUrl
        person_dict['_id'] = userId
        person_dict['location'] = location
        if gender == 'f':
            person_dict['gender'] = u'女'
        elif gender == 'm':
            person_dict['gender'] = u'男'
        else:
            person_dict['gender'] = u'未知'
        person_dict['verifiedType'] = verifiedType
        person_dict['verified'] = u'是' if verified else u'否'
        person_dict['created_at'] = ts2HMS(created_at)
        person_dict['domain'] = domain
        persons.append(person_dict)

    cobar_conn.close() 

    return persons


def _search_order_by_created_at(limit=1000, sharding=True):
    if sharding:
        cobar_conn = MySQLdb.connect(host=COBAR_HOST, user=COBAR_USER, db='cobar_db_weibo', port=COBAR_PORT, charset='utf8')
    else:
        cobar_conn = MySQLdb.connect(host='192.168.2.11', user='root', db='weibo', charset='utf8')

    persons = []
    cursor = cobar_conn.cursor()

    sql = "SELECT * from profile_person_basic ORDER BY created_at DESC LIMIT %d" % limit
    
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
    except:
        return persons

    for person in results:
        person_dict = {}
        _id, userId, province, city, verified, name, gender, profileImageUrl, verifiedType, friendsCount, followersCount, statuseCount, location, description, created_at, domain, date = person
        person_dict['statuses_count'] = statuseCount
        person_dict['followers_count'] = followersCount
        person_dict['friends_count'] = friendsCount
        person_dict['name'] = name
        person_dict['description'] = description
        person_dict['profile_image_url'] = profileImageUrl
        person_dict['_id'] = userId
        person_dict['location'] = location
        person_dict['gender'] = u'男' if gender == 'f' else u'女'
        person_dict['verifiedType'] = verifiedType
        person_dict['verified'] = u'是' if verified else u'否'
        person_dict['created_at'] = ts2HMS(created_at)
        person_dict['domain'] = domain
        persons.append(person_dict)

    cobar_conn.close() 

    return persons


def _search_by_domain(domain, limit=1000, sharding=True):
    if sharding:
        cobar_conn = MySQLdb.connect(host=COBAR_HOST, user=COBAR_USER, db='cobar_db_weibo', port=COBAR_PORT, charset='utf8')
    else:
        cobar_conn = MySQLdb.connect(host='192.168.2.11', user='root', db='weibo', charset='utf8')

    persons = []
    cursor = cobar_conn.cursor()

    sql = "SELECT * from profile_person_basic where domain='%s' ORDER BY followersCount DESC LIMIT %d" % (domain, limit)
    
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
    except Exception, e:
        print e
        return persons

    for person in results:
        person_dict = {}
        _id, userId, province, city, verified, name, gender, profileImageUrl, verifiedType, friendsCount, followersCount, statuseCount, location, description, created_at, domain, date = person
        person_dict['statuses_count'] = statuseCount
        person_dict['followers_count'] = followersCount
        person_dict['friends_count'] = friendsCount
        person_dict['name'] = name
        person_dict['description'] = description
        person_dict['profile_image_url'] = profileImageUrl
        person_dict['_id'] = userId
        person_dict['location'] = location
        person_dict['gender'] = u'男' if gender == 'f' else u'女'
        person_dict['verifiedType'] = verifiedType
        person_dict['verified'] = u'是' if verified else u'否'
        person_dict['created_at'] = ts2HMS(created_at)
        person_dict['domain'] = domain
        persons.append(person_dict)

    cobar_conn.close() 

    return persons
