# -*- coding: utf-8 -*-

import time
import datetime
from model import ProfilePersonBasic
from config import db, xapian_search_user

xapian_user_fields = ['_id', 'province', 'city', 'verified', 'name', 'friends_count', \
                     'gender', 'profile_image_url', 'verified_type','followers_count', \
                     'location', 'statuses_count', 'description', 'created_at']


def get_now_datestr():
    return datetime.datetime.now().strftime("%Y-%m-%d")


def _utf_decode(s):
    if isinstance(s, str):
        return s.decode('utf-8')
    else:
        return s


def iter_userbasic2mysql():
    users = xapian_search_user.iter_all_docs(fields=xapian_user_fields)

    count = 0
    ts = te = time.time()
    for user in users:
        if count % 10000 == 0:
            te = time.time()
            db.session.commit()
            print count, '%s sec' % (te - ts)
            ts = te

        try:
            userId = int(user['_id'])
        except:
            count += 1
            continue
        province = user['province']
        city = user['city']
        verified = user['verified']
        name = user['name']
        friendsCount = user['friends_count']
        gender = user['gender']
        profileImageUrl = user['profile_image_url']
        verifiedType = user['verified_type']
        followersCount = user['followers_count']
        location = user['location']
        statusesCount = user['statuses_count']
        description = _utf_decode(user['description'])

        try:
            created_at = int(user['created_at'])
        except:
            count += 1
            continue
        date = now_datetimestr

        '''
        print userId, province, city, verified, name, gender, profileImageUrl, verifiedType, friendsCount, followersCount, statusesCount, location, description, created_at, date
        print '--------'

        item_exist = db.session.query(ProfilePersonBasic).filter(ProfilePersonBasic.userId==userId).first()
        if item_exist:
            db.session.delete(item_exist)
        '''

        item = ProfilePersonBasic(userId, province, city, verified, name, gender, profileImageUrl, verifiedType, friendsCount, followersCount, statusesCount, location, description, created_at, date)
        db.session.add(item)

        count += 1


if __name__ == '__main__':
    now_datetimestr = get_now_datestr()

    iter_userbasic2mysql()
