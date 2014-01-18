#-*- encoding: utf-8 -*-
import os, sys, string
import MySQLdb


# 连接数据库　
try:
    #conn = MySQLdb.connect(host='192.168.2.11', user='cobar', db='cobar_db_weibo', port=8066)
    #conn = MySQLdb.connect(host='192.168.2.10', user='root', db='cobar_weibo')
    conn = MySQLdb.connect(host='192.168.2.21', user='root', db='cobar_weibo')
    print 'connection success'
except Exception, e:
    print e
    sys.exit()
'''
try:
    read_conn = MySQLdb.connect(host='192.168.2.11', user='root', db='weibo')
    print 'connection success'
except Exception, e:
    print e
    sys.exit()
'''

# 获取cursor对象来进行操作
#read_cursor = read_conn.cursor()
cursor = conn.cursor()

# 创建表
'''
sql = "CREATE TABLE if not exists profile_person_basic_test ( \
  id int(11) NOT NULL AUTO_INCREMENT, \
  userId bigint(20) DEFAULT NULL, \
  province int(11) DEFAULT NULL, \
  city int(11) DEFAULT NULL, \
  verified tinyint(1) DEFAULT NULL, \
  name text, \
  gender varchar(2) DEFAULT NULL, \
  profileImageUrl varchar(100) DEFAULT NULL, \
  verifiedType int(11) DEFAULT NULL, \
  friendsCount bigint(20) DEFAULT NULL, \
  followersCount bigint(20) DEFAULT NULL, \
  statuseCount bigint(20) DEFAULT NULL, \
  location varchar(20) DEFAULT NULL, \
  description text, \
  created_at bigint(20) NULL, \
  date date DEFAULT NULL, \
  PRIMARY KEY (id) \
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;"
'''
sql = "CREATE TABLE profile_person_weibo_count ( \
  id int(11) NOT NULL AUTO_INCREMENT, \
  userId bigint(20) DEFAULT NULL, \
  endDate date DEFAULT NULL, \
  active bigint(20) DEFAULT NULL, \
  important bigint(20) DEFAULT NULL, \
  reposts bigint(20) DEFAULT NULL, \
  original bigint(20) DEFAULT NULL, \
  emoticon bigint(20) DEFAULT NULL, \
  PRIMARY KEY (id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8;"

cursor.execute(sql)
'''
# 插入数据
sql = "insert into bt_today_top values ('%s', '%s', %d, '%s')" % ("[大陆][09DOTA零单高分局新船长和前剧情补完 超清][无需字幕][720P][AVI]","视频",237,"09高分局船长")
try:
    cursor.execute(sql)
except Exception, e:
    print e

sql = "insert into bt_today_top values ('%s', '%s', %d, '%s')" % ("[台湾][第50届金马奖颁奖典礼][20131123][历届影帝影后齐聚金马][无需字幕][RMVB][RMVB]","综艺",217,"都说黄渤机智，反击了下蔡康永，看看")
try:
    cursor.execute(sql)
except Exception, e:
    print e

# 插入多条
sql = "insert into bt_today_top values (%s, %s, %s, %s)" 
val = (("[大陆][咱们结婚吧][We Get Married][E25~E26][中文字幕][720P]", "剧集", 24, "由完美蓬瑞、华录百纳、完美影视出品的2013年度压轴爱情大戏《咱们结婚吧》该剧由曾经创作《媳妇的美好时代》的知名导演刘江执导，高圆圆、黄海波、王彤、凯丽、大左等演员联袂主演，强大阵容、精彩剧情都让很多观众期待不已"), ("[足球][2013-11-24][德甲第13轮 国家德比 多特蒙德vs拜仁][中文][720P][MKV]", "体育", 63, "【90分钟足球网】2013-11-24德甲多特蒙德vs拜仁720P高清央视版"), ("[日漫][连载][海贼王][One Piece][622][中文字幕][枫雪][TVRip][H264]", "动漫", 60, "海贼王，每个人小时候的梦想"))
try:
    cursor.executemany(sql, val)
except Exception, e:
    print e

#查询出数据
sql = "select * from profile_person_basic"
# cursor.execute(sql)
# alldata = cursor.fetchall()

read_cursor.execute(sql)
one_record = read_cursor.fetchone()

read_data = cursor.fetchall()

for record in read_data:
    print record

# 如果有数据返回，就循环输出, alldata是有个二维的列表
if alldata:
    for rec in alldata:
        print rec

#commit才能插入数据
#conn.commit()

cursor.close()
conn.close()

read_cursor.close()
read_conn.close()

'''
'''
def test_sqlalchemy():
    from database import db_session
    from models import ProfilePersonBasic
    person = db_session.query(ProfilePersonBasic).filter_by(userId=2015464173).first()
    print person.name
    db_session.remove()
    #print ProfilePersonBasic.query.all().count()

if __name__ == '__main__':
    test_sqlalchemy()
'''
