# -*- coding: utf-8 -*-

import time
import json
from flask import Flask, request
from optparse import OptionParser
from dynamic_xapian_weibo import target_whole_xapian_weibo, getXapianWeiboByDuration
from flask.ext.restful import Resource, Api, reqparse, abort


TODOS = {
    'todo1': {'task': 'build an API'},
    'todo2': {'task': '?????'},
    'todo3': {'task': 'profit!'},
}

XAPIAN_WEIBO_FIELDS = ['_id', 'user', 'retweeted_uid', 'retweeted_mid', \
                       'text', 'timestamp', 'reposts_count', \
                       'bmiddle_pic', 'geo', 'comments_count', \
                       'sentiment', 'terms']


def abort_if_todo_doesnt_exist(todo_id):
    if todo_id not in TODOS:
        abort(404, message="Todo {} doesn't exist".format(todo_id))

# Todo
#   show a single todo item and lets you delete them
class Todo(Resource):
    def get(self, todo_id):
        abort_if_todo_doesnt_exist(todo_id)
        return TODOS[todo_id]

    def delete(self, todo_id):
        abort_if_todo_doesnt_exist(todo_id)
        del TODOS[todo_id]
        return '', 204

    def put(self, todo_id):
        args = parser.parse_args()
        task = {'task': args['task']}
        TODOS[todo_id] = task
        return task, 201


# TodoList
#   shows a list of all todos, and lets you POST to add new tasks
class TodoList(Resource):
    def get(self):
        return TODOS

    def post(self):
        args = parser.parse_args()
        todo_id = 'todo%d' % (len(TODOS) + 1)
        TODOS[todo_id] = {'task': args['task']}
        return TODOS[todo_id], 201


def abort_if_mid_doesnt_exist(mid):
    if not mid or mid == '':
        abort(404, message="Mid {} doesn't exist".format(mid))

    try:
        mid = int(mid)
    except:
        abort(404, message="Mid {} is not an integer".format(mid))


def abort_if_ts_isnot_integer(start_ts, end_ts):
    try:
        start_ts = int(start_ts)
    except:
        abort(404, message="start ts {} is not an integer".format(start_ts))

    try:
        end_ts = int(end_ts)
    except:
        abort(404, message="end ts {} is not an integer".format(end_ts))


def datetimestr2ts(date):
    return int(time.mktime(time.strptime(date, '%Y%m%d')))


def ts2datetimestr(ts):
    return time.strftime('%Y%m%d', time.localtime(ts))


class StatusExist(Resource):
    def get(self, mid, start_ts=None, end_ts=None):
        abort_if_mid_doesnt_exist(mid)

        if start_ts or end_ts:
            abort_if_ts_isnot_integer(start_ts, end_ts)

            start_ts = int(start_ts)
            end_ts = int(end_ts)
            start_ts_0 = datetimestr2ts(ts2datetimestr(start_ts))
            end_ts_24 = datetimestr2ts(ts2datetimestr(end_ts)) + 24 * 3600
            
            datelist = []

            for ts in range(start_ts_0, end_ts_24, 24 * 3600):
                datelist.append(ts2datetimestr(ts))

            xapian_weibo = getXapianWeiboByDuration(datelist)
            status = xapian_weibo.search_by_id(int(mid), fields=XAPIAN_WEIBO_FIELDS)

        else:
            status = whole_xapian_weibo.search_by_id(int(mid), fields=XAPIAN_WEIBO_FIELDS)

        if status:
            weibo = {}
            for field in XAPIAN_WEIBO_FIELDS:
                weibo[field] = status[field]
            return {'status': 'true', 'data': weibo}
        else:
            return {'status': 'false', 'data': ''}


if __name__ == '__main__':
    optparser = OptionParser()
    optparser.add_option('-p', '--port', dest='port', help='Server Http Port Number', default=9004, type='int')
    (options, args) = optparser.parse_args()

    parser = reqparse.RequestParser()
    parser.add_argument('task', type=str)

    app = Flask(__name__)
    api = Api(app)

    whole_xapian_weibo = target_whole_xapian_weibo()

    ##
    ## Actually setup the Api resource routing here
    ##
    api.add_resource(TodoList, '/todos')
    api.add_resource(Todo, '/todos/<string:todo_id>')
    api.add_resource(StatusExist, \
                     '/status_exist/<string:mid>', \
                     '/status_exist/<string:mid>/<string:start_ts>/<string:end_ts>')

    app.run(debug=True, host='0.0.0.0', port=options.port)
