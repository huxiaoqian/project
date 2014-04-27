# -*- coding: utf-8 -*-

import happybase

connection = happybase.Connection('localhost', 9090)
table = connection.table('test')

table.put('row-key', {'cf:qual1': 'value1',
                      'cf:qual2': 'value2'})

row = table.row('row-key')
print row['cf:qual1']  # prints 'value1'

for key, data in table.rows(['row-key-1', 'row-key-2']):
    print key, data  # prints row key and data for each row

for key, data in table.scan(row_prefix='row'):
    print key, data  # prints 'value1' and 'value2'

row = table.delete('row-key')
