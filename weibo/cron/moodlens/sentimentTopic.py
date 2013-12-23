# -*- coding: utf-8 -*-
'''定制话题数量、关键词、关键微博数据管理
'''

import time
from config import db
from model import SentimentTopicCount, SentimentTopicKeywords, SentimentTopicTopWeibos


def _drop_sentiment_topic_count(topicname):
  items = db.session.query(SentimentTopicCount).filter(SentimentTopicCount.query==topicname).all()
  for item in items:
  	db.session.delete(item)
  db.session.commit()


def _drop_sentiment_topic_keywords(topicname):
  items = db.session.query(SentimentTopicKeywords).filter(SentimentTopicKeywords.query==topicname).all()
  for item in items:
  	db.session.delete(item)
  db.session.commit()


def _drop_sentiment_topic_weibos(topicname):
  items = db.session.query(SentimentTopicTopWeibos).filter(SentimentTopicTopWeibos.query==topicname).all()
  for item in items:
  	db.session.delete(item)
  db.session.commit()
