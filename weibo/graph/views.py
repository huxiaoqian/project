# -*- coding: utf-8 -*-

import json
import math
from graph import getWeiboByMid, graph_from_elevator
from flask import Blueprint, session, render_template, redirect, \
                  url_for, jsonify, make_response

mod = Blueprint('graph', __name__, url_prefix='/gexf')


@mod.route('/show_graph/<int:mid>/')
@mod.route('/show_graph/<int:mid>/<int:page>/')
def show_graph_index(mid, page=None):
    if page is None:
        per_page = 200

        # weibo
        weibo = getWeiboByMid(mid)
        try:
            retweeted_mid = weibo['retweeted_mid']
        except:
            return json.dumps('No such mid')

        # source_weibo
        source_weibo = weibo
        if retweeted_mid != 0:
            source_weibo = getWeiboByMid(retweeted_mid)

        #reposts_count = source_weibo['reposts_count']
        #total_page = int(math.ceil(reposts_count * 1.0 / per_page))
        #page = total_page
        page = 0

        return redirect('/gexf/show_graph/%s/%s/'%(mid, page))#{url_for(graph.show_graph(mid, page))})

    screen_name = 'nobody'
    profile_image_url = 'http://www.baidu.com'
    return render_template('graph/graph.html', btnuserpicvisible='inline',
                           btnloginvisible='none',
                           screen_name=screen_name, profile_image_url=profile_image_url,
                           mid=mid,
                           page=page)

@mod.route('/graph/<int:mid>/')
@mod.route('/graph/<int:mid>/<int:page>/')
def graph_index(mid, page=None):
    module = request.args.get('module', 'sub')
    g = graph_from_elevator(mid)

    if not g:
        g = ''

    result = g.split('_\/')
    if len(result) > 1:
        if module == 'sub':
            tree_g = result[2]
            tree_stats = json.loads(result[3])
        else:
            tree_g = result[0]
            tree_stats = json.loads(result[1])

        response = make_response(tree_g)
        response.headers['Content-Type'] = 'text/xml'

        return response
    else:
        response = make_response('')
        response.headers['Content-Type'] = 'text/xml'

        return response

@mod.route('/tree_stats/<int:mid>/<int:page>/')
def tree_stats_index(mid, page):
    module = request.args.get('module', 'sub')
    g = graph_from_elevator(mid)

    if not g:
        g = ''

    result = g.split('_\/')
    if len(result) > 1:
        if module == 'sub':
            tree_g = result[2]
            tree_stats = json.loads(result[3])
        else:
            tree_g = result[0]
            tree_stats = json.loads(result[1])
    else:
        tree_stats = {}

    tree_stats['spread_begin'] = tree_stats['spread_begin'].strftime('%Y-%m-%d %H:%M:%S')
    tree_stats['spread_end'] = tree_stats['spread_end'].strftime('%Y-%m-%d %H:%M:%S')

    return jsonify(stats=tree_stats)
