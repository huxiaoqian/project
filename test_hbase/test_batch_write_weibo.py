# -*- coding: utf-8 -*-

import os
import time
import happybase
from datetime import datetime
from xapian_weibo.csv2json import itemLine2Dict

#
XAPIAN_FLUSH_DB_SIZE = 1000
CSV_FILEPATH = '/media/data/original_data/csv/20130923_cut/'

# connect to weibo table
connection = happybase.Connection('localhost', 9090)
table = connection.table('weibo')

RESP_ITER_KEYS = ['_id', 'user', 'retweeted_uid', 'retweeted_mid', 'text',
                  'timestamp', 'reposts_count', 'bmiddle_pic',
                  'geo', 'comments_count']


def load_items_from_csv(csv_filepath):
    print 'csv file mode: 从CSV文件中加载数据'
    csv_input = open(csv_filepath)
    return csv_input


def csv_input_pre_func(item):
    item = itemLine2Dict(item)
    return item


def hbase_single_write(load_origin_data_func, pre_funcs=[]):
    count = 0
    tb = time.time()
    ts = tb
    for item in load_origin_data_func():
        if pre_funcs:
            for func in pre_funcs:
                item = func(item)
        if item is None or item == "":
            continue
        
        column_value_dict = {} 
        rowkey = str(item['_id'])
        column_family = 'o'
        for column_key in RESP_ITER_KEYS:
            column = '%s:%s' % (column_family, column_key)
            value = item[column_key]
            try:
                value = str(value)
            except:
                value = str(value.encode('utf-8'))
            column_value_dict[column] = value

        table.put(rowkey, column_value_dict)
        #print table.row(rowkey)

        count += 1
        if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
            te = time.time()
            print '[%s] deliver speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, XAPIAN_FLUSH_DB_SIZE * 10)
            if count % (XAPIAN_FLUSH_DB_SIZE * 100) == 0:
                print '[%s] total deliver %s, cost: %s sec [avg: %sper/sec]' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), count, te - tb, count / (te - tb))
            ts = te
            
    total_cost = time.time() - tb
    return count, total_cost


def hbase_batch_write(load_origin_data_func, pre_funcs=[], batch=10000):
    b = table.batch()
    count = 0
    tb = time.time()
    ts = tb
    for item in load_origin_data_func():
        if pre_funcs:
            for func in pre_funcs:
                item = func(item)
        if item is None or item == "":
            continue
        
        column_value_dict = {} 
        rowkey = str(item['_id'])
        column_family = 'o'
        for column_key in RESP_ITER_KEYS:
            column = '%s:%s' % (column_family, column_key)
            value = item[column_key]
            try:
                value = str(value)
            except:
                value = str(value.encode('utf-8'))
            column_value_dict[column] = value

        b.put(rowkey, column_value_dict)
        
        count += 1
        if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
            te = time.time()
            print '[%s] deliver speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, XAPIAN_FLUSH_DB_SIZE * 10)
            if count % (XAPIAN_FLUSH_DB_SIZE * 100) == 0:
                print '[%s] total deliver %s, cost: %s sec [avg: %sper/sec]' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), count, te - tb, count / (te - tb))
            ts = te

        if count % batch == 0:
            b.send()
            b = table.batch()
            
    total_cost = time.time() - tb
    return count, total_cost


def test_write_single():
    files = os.listdir(CSV_FILEPATH)
    pre_funcs = [csv_input_pre_func]
    total_cost = 0
    count = 0
    for f in files:
        csv_input = load_items_from_csv(os.path.join(CSV_FILEPATH, f))
        load_origin_data_func = csv_input.__iter__
        tmp_count, tmp_cost = hbase_single_write(load_origin_data_func, pre_funcs=pre_funcs)
        total_cost += tmp_cost
        count += tmp_count
        csv_input.close()
    
    print 'write complete, total count %s, total_cost %s' % (count, total_cost)


def test_write_batch(batch=10000):
    files = os.listdir(CSV_FILEPATH)
    pre_funcs = [csv_input_pre_func]
    total_cost = 0
    count = 0
    for f in files:
    	  print f
        csv_input = load_items_from_csv(os.path.join(CSV_FILEPATH, f))
        load_origin_data_func = csv_input.__iter__
        tmp_count, tmp_cost = hbase_batch_write(load_origin_data_func, pre_funcs=pre_funcs, batch=batch)
        total_cost += tmp_cost
        count += tmp_count
        csv_input.close()
        break
    
    print 'batch count %s' % batch
    print 'write complete, total count %s, total_cost %s' % (count, total_cost)


if __name__ == '__main__':
	  # 1 node: 202.108.211.5

    # 20 sec per 10000, 500 per second
    # test_write_single()
    
    # batch 5000, total count 698935, total cost 179.405210972, 3895 per second
    # test_write_batch(5000)

    # batch 6000, total count 698935, total cost 167.044354916, 4184 per second
    # test_write_batch(6000)

    # batch 7000, total count 698935, total cost 164.847719193, 4239 per second
    # test_write_batch(7000)
    
    # batch 8000, total count 698935, total cost 163.231114864, 4281 per second
    test_write_batch(8000)
    
    # batch 9000, total count 698935, total cost 166.424035072, 4199 per second
    # test_write_batch(9000)

    # batch 10000, total count 698935, total cost 166.81230998, 4189 per second
    # test_write_batch()

    # batch 20000, total count 698935, total cost 173.607800961, 4025 per second
    # test_write_batch(20000)
    
    # batch 25000, total count 698935, total cost 172.483246088, 4052 per second
    # test_write_batch(25000)

    # batch 30000, total count 698935, total cost 177.512379169, 3927 per second
    # test_write_batch(30000)
