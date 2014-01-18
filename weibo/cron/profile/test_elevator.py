# -*- coding: utf-8 -*-

import time
import pyelevator
from pyelevator import WriteBatch, Elevator

# Elevator server holds a default db
# which the client will automatically
# connect to

E = Elevator(transport='tcp', endpoint='192.168.2.11:4141')

ts = te = time.time()
count = 0
'''
batch = E.WriteBatch()
for i in range(0, 1000001):
    if count % 10000 == 0:
        te = time.time()
        batch = E.WriteBatch()
        print count, '%s sec' % (te - ts)
        ts = te

    batch.Put(str(i), str(i))
    count += 1
'''

ts = te = time.time()
count = 0
#print E.Slice('1', 10)
for k, v in E.RangeIter('1', '3'):
    if count % 10000 == 0:
        te = time.time()
        print count, '%s sec' % (te - ts)
        ts = te

    count += 1

'''
print 'default db name: ', E.db_name

# You can list remote databases
print 'list remote databases: ', E.listdb()

# Create a db
try:
		E.createdb('testdb')
except Exception, e:
		print e
print 'list remote databases: ', E.listdb()

# And bind your client to that new Db.
E.connect('default')

# Note that you canno't connect to a db that doesn't exist yet
# DatabaseError : "Database does not exist"
try:
		E.connect('dbthatdoesntexist')
except Exception, e:
		print e

# Sometimes, leveldb just messes up with the backend
# When you're done with a db, you can drop it. Note that all it's files
# will be droped too.
try:
		E.repairdb('testdb')
		E.dropdb('testdb')
except Exception, e:
		print e
print 'list remote databases: ', E.listdb()
'''

# You can even register a pre-existing leveldb db
# as an Elevator db. By creating it using it's path.
#E.dropdb('/home/mirage/leveldb/linhao_profile_person_20130901_backup')
'''
try:
		E.createdb('/home/mirage/leveldb/linhao_profile_person_count_20130901_backup')
except Exception, e:
		print e
print E.listdb()
'''

'''
# Elevator objects can also spawn WriteBatches objects,
# inheriting it's parent Elevator object configuration.
try:
		batch = E.WriteBatch()
except Exception, e:
		print e
'''

E.connect('/home/mirage/leveldb/linhao_profile_person_count_20130901_backup')
#E.repairdb('/home/mirage/leveldb/linhao_profile_person_count_20130901_backup')
ts = te = time.time()
count = 0
for k, v in E.RangeIter('1684502353', '1684522353'):
    print k
    if count % 10000 == 0:
        te = time.time()
        print count, '%s sec' % (te - ts)
        ts = te
    count += 1

    #print k, v

E.disconnect('/home/mirage/leveldb/linhao_profile_person_count_20130901_backup')
