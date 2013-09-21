# -*- coding: utf-8 -*-
import time
from view import app, getFieldTopics, getHotStatus

app = app.test_client()


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.4f sec' % (method.__name__, te-ts)
        return result
    return timed

@timeit
def test_get_index_page():
	#field_topics = getFieldTopics()
	#print field_topics
	status_hot = getHotStatus(40)

@timeit
def test_get_topic_result(test='None'):
	rv = app.get('/showresult/?keyword=两会&test=' + test)
	print rv.data

@timeit
def test_get_single_result(test='None', mid=None):
	print '/showresult_single/' + str(mid) + '/?test=' + test
	rv = app.get('/showresult_single/' + str(mid) + '/?test=' + test)
	print rv.data


def main():
	'''传播模块测试
	'''
	# #time: 0.0374s(获取领域下的话题信息，1个领域下的3个话题)
	# #time: 0.0425s(获取5条微博)，0.2087s(获取10条微博)，0.3747s(获取20条微博)，0.7544s(获取40条微博)
	# test_get_index_page()

	# #0.6884s(检索话题两会的分析结果，检索范围是2亿5千万，命中数为434)
	# #2.3441s(检索话题春节的分析结果，检索范围是2亿5千万，命中数为2313)
	# #2.2965s(检索话题两会的分析结果，检索范围是10亿，命中数为1165)
	# #7.6856s(检索话题春节的分析结果，检索范围是10亿，命中数为6124)
	# test_get_topic_result()
	# test_get_topic_result('test')

	# #0.0762s(检索范围是2亿5千万)
	# #
	# test_get_single_result('None', 3525579443620038)
	test_get_single_result('test', 3525579443620038)


if __name__ == '__main__':
	main()

