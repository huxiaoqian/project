# -*- coding: utf-8 -*-


import time
from config import db, cron_end
from model import TopicStatus
from time_utils import datetime2ts

NOT_CALC_STATUS = -1
IN_CALC_STATUS = 0
COMPLETED_STATUS = 1

Minute = 60
Fifteenminutes = 15 * 60
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24


def _topic_not_calc(status=NOT_CALC_STATUS, module='sentiment'):
    topics_list = []
    topics = TopicStatus.query.filter_by(status=status, module=module).all()
    for topic in topics:
        topics_list.append(topic)

    return topics_list


def _topic_calculating(status=IN_CALC_STATUS, module='sentiment'):
    topics_list = []
    topics = TopicStatus.query.filter_by(status=status, module=module).all()
    for topic in topics:
        topics_list.append(topic.topic)

    return topics_list


def _topic_completed(status=COMPLETED_STATUS, module='sentiment'):
    topics_list = []
    topics = TopicStatus.query.filter_by(status=status, module=module).all()
    for topic in topics:
        topics_list.append(topic.topic)

    return topics_list


def _add_not_start_topic(topic, status=NOT_CALC_STATUS, end=datetime2ts(cron_end), range=Fifteenminutes, module='sentiment'):
    pass
    '''
    item = TopicStatus(module, status, start, end, topic, end, range,)
    item_exist = TopicStatus.query.filter_by(module=module, topic=topic, status=status, end=end, range=range).first()
    if item_exist:
        db.session.delete(item_exist)
    db.session.add(item)
    db.session.commit()
    '''


def _update_topic_status2Computing(topic, start, end, db_date, range=Fifteenminutes, module='sentiment'):
    item = TopicStatus(module, IN_CALC_STATUS, topic, start, end, range, db_date)
    item_exist = db.session.query(TopicStatus).filter(TopicStatus.module==module, \
                                                      TopicStatus.status==NOT_CALC_STATUS, \
                                                      TopicStatus.topic==topic, \
                                                      TopicStatus.start==start, \
                                                      TopicStatus.end==end, \
                                                      TopicStatus.range==range, \
                                                      TopicStatus.db_date==db_date).first()
    if item_exist:
        db.session.delete(item_exist)
    db.session.add(item)
    db.session.commit()


def _update_topic_status2Completed(topic, start, end, db_date, range=Fifteenminutes, module='sentiment'):
    item = TopicStatus(module, COMPLETED_STATUS, topic, start, end, range, db_date)
    item_exist = db.session.query(TopicStatus).filter(TopicStatus.module==module, \
                                                      TopicStatus.status==IN_CALC_STATUS, \
                                                      TopicStatus.topic==topic, \
                                                      TopicStatus.start==start, \
                                                      TopicStatus.end==end, \
                                                      TopicStatus.range==range, \
                                                      TopicStatus.db_date==db_date).first()
    if item_exist:
        db.session.delete(item_exist)
    db.session.add(item)
    db.session.commit()


def _drop_item(topic, start, end, db_date, status, range=Fifteenminutes, module='sentiment'):
    item_exist = db.session.query(TopicStatus).filter(TopicStatus.module==module, \
                                                      TopicStatus.status==IN_CALC_STATUS, \
                                                      TopicStatus.topic==topic, \
                                                      TopicStatus.start==start, \
                                                      TopicStatus.end==end, \
                                                      TopicStatus.range==range, \
                                                      TopicStatus.db_date==db_date).first()
    if item_exist:
        db.session.delete(item_exist)
    db.session.commit()


if __name__ == '__main__':
    pass
    #_update_topic_status2Completed('高考', 1377964800, 1378051200)
    #_drop_item('日本', 1377964800, 1378310400, IN_CALC_STATUS)
    #_add_not_start_topic('开学')
