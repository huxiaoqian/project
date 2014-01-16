# -*- coding: utf-8 -*-

import time
import MySQLdb
from weibo.extensions import db
from weibo.model import ProfilePersonBasic, ProfilePersonWeiboCount
from weibo.global_config import COBAR_HOST, COBAR_PORT, COBAR_USER
            

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

    def __init__(self, person_tuple):
        userId, province, city, verified, name, gender, profileImageUrl, verifiedType, friendsCount, followersCount, statuseCount, location, description, created_at, date = person_tuple
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
        self.description = description    


def _search_person_basic(userId, sharding=False):
    if sharding:
        person = None
        cobar_conn = MySQLdb.connect(host=COBAR_HOST, user=COBAR_USER, db='cobar_db_weibo', port=COBAR_PORT, charset='utf8')
        cursor = cobar_conn.cursor()
        sql = "SELECT * from profile_person_basic where userId = %d " % int(userId)

        try:
            cursor.execute(sql)
            person = cursor.fetchone()
        except:
            pass

        if person:
            person_tuple = (person[1], person[2], person[3], person[4], person[5], \
                            person[6], person[7], person[8], person[9], person[10], \
                            person[11], person[12], person[13], person[14], person[15])
            person = Person(person_tuple)

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
