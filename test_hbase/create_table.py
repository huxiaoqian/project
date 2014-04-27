# -*- coding: utf-8 -*-

import happybase

thrift_server_host = '192.168.2.31'
thrift_server_port = 9090
connection = happybase.Connection(thrift_server_host, thrift_server_port)

# 创建table 'weibo', Column family 'o', Column famliy需要在创建table时指定
connection.create_table(
    'weibo',
    {
     'o': dict(),  # use defaults
    }
)
