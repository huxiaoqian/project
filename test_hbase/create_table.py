# -*- coding: utf-8 -*-

import happybase

connection = happybase.Connection('localhost', 9090)

# 创建table 'weibo', Column family 'o', Column famliy需要在创建table时指定
connection.create_table(
    'weibo_test',
    {
     'o': dict(),  # use defaults
    }
)
