# -*- coding: utf-8 -*-

import os
import sys
import re
import calendar
import time
from datetime import date
from datetime import datetime

from model import *
from config import db

def readPropagateWeibo(keyid):

    topic_info = []
    propagateWeibos = db.session.query(PropagateWeibo).filter(PropagateWeibo.topic_id==keyid).all()
    
    if len(propagateWeibos):
        
        for p in propagateWeibos:
            user = dict()
            status = dict()
            status['_id'] = p.mid
            user['profile_image_url'] = p.image_url
            status['text'] = p.text
            status['source'] = p.sourcePlatform
            status['created_at'] = p.postDate
            user['id'] = p.uid
            user['name'] = p.user_name
            status['reposts_count'] = p.repostsCount
            status['comments_count'] = p.commentsCount
            status['attitudes_count'] = p.attitudesCount
            topic_info.append({'status':status,'user':user})

        return topic_info
    else:
        return topic_info

