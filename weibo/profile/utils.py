# -*- coding: utf-8 -*-

import time
from datetime import datetime, timedelta, date

from config import db
from config import app as current_app
from flask import request
from model import *
from sqlalchemy import func

import json

def local2datetime(time_str):
    time_format = '%a %b %d %H:%M:%S +0800 %Y'
    return datetime.fromtimestamp(int(time.mktime(time.strptime(time_str, time_format))))

def ts2datetime(ts):
     return datetime.fromtimestamp(int(float(ts)))

def time2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def hot_uid_by_word(starttime, endtime, count=50):
    '''筛选出词语表中有超过50条记录的博主
    '''
    startdate = ts2datetime(time2ts(starttime))
    enddate =  ts2datetime(time2ts(endtime))
    uids = set()
    uids_count = db.session.query(Words.uid, func.count(Words.id)).\
                               filter(Words.postDate>startdate, Words.postDate<enddate).\
                               group_by(Words.uid).\
                               having(func.count(Words.id) > count).all()
    for uid, count in uids_count:
        uids.add(uid)
    return uids

def last_week():
    '''计算当前日期的上一周起止日期（从上周一到本周一）
    '''
    now_weekday = datetime.now().weekday()
    now_date = date.today()
    this_monday = now_date - timedelta(days=now_weekday)
    last_monday = this_monday - timedelta(days=7)
    return last_monday.isoformat(), this_monday.isoformat()

def last_month():
    '''计算当前日期回溯30日
    '''
    now_date = date.today()
    last_date = now_date - timedelta(days=30)
    return last_date.isoformat(), now_date.isoformat()

class Serializer(object):
  __public__ = None
  "Must be implemented by implementors"

  def to_serializable_dict(self):
    dict = {}
    for public_key in self.__public__:
      value = getattr(self, public_key)
      if value:
        dict[public_key] = value
    return dict

class SWEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, Serializer):
      return obj.to_serializable_dict()
    if isinstance(obj, (datetime)):
      return obj.isoformat()
    return json.JSONEncoder.default(self, obj)


def SWJsonify(*args, **kwargs):
  return current_app.response_class(json.dumps(dict(*args, **kwargs), cls=SWEncoder, indent=None if request.is_xhr else 2), mimetype='application/json')
  # stolen from https://github.com/mitsuhiko/flask/blob/master/flask/helpers.py

##from sqlalchemy.ext.declarative import DeclarativeMeta
##class AlchemyEncoder(json.JSONEncoder):
##    def default(self, obj):
##        if isinstance(obj.__class__, DeclarativeMeta):
##            # an SQLAlchemy class
##            fields = {}
##            for field in [x for x in dir(obj) if not x.startswith('_') and x != 'metadata']:
##                data = obj.__getattribute__(field)
##                try:
##                    json.dumps(data) # this will fail on non-encodable values, like other classes
##                    fields[field] = data
##                except TypeError:
##                    fields[field] = None
##            # a json-encodable dict
##            return fields
##
##        return json.JSONEncoder.default(self, obj)
    
def main():
    last_week()
    pass
    
if __name__ == '__main__': main()
            
