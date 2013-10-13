#-*- coding:utf-8 -*-
"""Read database within readonly functionality"""

import pymongo

def getReadonlyDB(targetDB):
    DB_USER_NAME = 'readonly'
    DB_USER_PWD = 'readonly'
    connection = pymongo.Connection("219.224.135.60")
    db = connection.admin
    db.authenticate(DB_USER_NAME, DB_USER_PWD)
    db = connection[targetDB]
    return db

if __name__ == '__main__':
    # the example below is to query count of the "users" collection of database "weibo"
    db = getReadonlyDB('weibo')
    count = db.users.find().count()
    print count
