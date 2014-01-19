# -*- coding: utf-8 -*-

import json
import math
from graph import getWeiboByMid, graph as _graph
from flask import Blueprint, session, render_template, redirect, url_for, jsonify

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

        reposts_count = source_weibo['reposts_count']
        total_page = int(math.ceil(reposts_count * 1.0 / per_page))
        page = total_page

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
    return _graph(mid)['graph']
    '''
    per_page = 200
    total_page = 0
    reposts_count = 0
    source_weibo = None
    if page is None:
        source_weibo = client.get('statuses/show', id=mid)
        mongo.db.all_source_weibos.update({'id': source_weibo['id']}, source_weibo, upsert=True)

        items2mongo(resp2item_v2(source_weibo))

        reposts_count = source_weibo['reposts_count']
        total_page = int(math.ceil(reposts_count * 1.0 / per_page))
        page = total_page
    else:
        source_weibo = mongo.db.all_source_weibos.find_one({'id': mid})
        if source_weibo is None:
            return ''
        reposts_count = source_weibo['reposts_count']
        total_page = int(math.ceil(reposts_count * 1.0 / per_page))

    try:
        reposts = client.get('statuses/repost_timeline', id=mid,
                             count=200, page=page)['reposts']

        # 如果reposts为空，且是最开始访问的一页，有可能是页数多算了一页,直接将页数减一页跳转
        if reposts == [] and total_page > 1 and page == total_page:
            return redirect(url_for('graph.index', mid=mid, page=page - 1))

        items = []
        for repost in reposts:
            items.extend(resp2item_v2(repost))
        items2mongo(items)
        for item in items:
            if isinstance(item, WeiboItem) and item['id'] != source_weibo['id']:
                item = item.to_dict()
                item['source_weibo'] = source_weibo['id']
                mongo.db.all_repost_weibos.update({'id': item['id']}, item, upsert=True)
    except RuntimeError:
        pass

    reposts = list(mongo.db.all_repost_weibos.find({'source_weibo': source_weibo['id']}))
    if reposts == []:
        return ''

    page_count = total_page - page + 1 if total_page >= page else 0
    tree, tree_stats = reposts2tree(source_weibo, reposts, per_page, page_count)
    graph, max_depth, max_width = tree2graph(tree)
    tree_stats['max_depth'] = max_depth
    tree_stats['max_width'] = max_width

    # 存储转发状态
    tree_stats['id'] = mid
    tree_stats['page'] = page
    mongo.db.tree_stats.update({'id': mid, 'page': page}, tree_stats, upsert=True, w=1)
    return graph
    '''

@mod.route('/tree_stats/<int:mid>/<int:page>/')
def tree_stats_index(mid, page):
    tree_stats = _graph(mid)['stats']
    tree_stats['spread_begin'] = tree_stats['spread_begin'].strftime('%Y-%m-%d %H:%M:%S')
    tree_stats['spread_end'] = tree_stats['spread_end'].strftime('%Y-%m-%d %H:%M:%S')

    return jsonify(stats=tree_stats)
