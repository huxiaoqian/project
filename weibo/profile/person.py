# -*- coding: utf-8 -*-

import time
from weibo.extensions import db
from weibo.model import ProfilePersonBasic, ProfilePersonWeiboCount

def _search_person_basic(userId):
    person = ProfilePersonBasic.query.filter_by(userId=userId).first()
    print userId
    if person:
        return 'success', person
    else:
        return 'failed', None


def _search_person_important_active(userId):
    person = ProfilePersonWeiboCount.query.filter_by(userId=userId).first()
    if person:
        return 'success', person
    else:
        return 'failed', None
