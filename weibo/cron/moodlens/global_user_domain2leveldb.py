# -*- coding: utf-8 -*-

import os
import sys
import json
import leveldb
import datetime
from xapian_weibo.utils import load_scws
from config import xapian_search_user, LEVELDBPATH

UPDATE_TIME = '20131220'
STATUS_THRE = 4000
FOLLOWER_THRE = 1000

labels = ['university', 'homeadmin', 'abroadadmin', 'homemedia', 'abroadadmin', 'folkorg', \
          'lawyer', 'politician', 'mediaworker', 'activer', 'grassroot', 'other']
zh_labels = ['高校微博', '境内机构', '境外机构', '媒体', '境外媒体', '民间组织', '律师', \
             '政府官员', '媒体人士', '活跃人士', '草根']
outlist = ['海外', '香港', '台湾', '澳门']
lawyerw = ['律师', '法律', '法务', '辩护']

spieduser = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'spiedusers'),
                            block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))



adminf=open('adw.txt','r')
adminw=[]
noreason=0##无认证原因
while True:
	line=adminf.readline()
	if not line:
		break
	else:
		record=line.split()
		adminw.append(record[0])##政府职位相关词汇
mediaf=open('mediaw.txt','r')
mediaw=[]
while True:
	line=mediaf.readline()
	if not line:
		break
	else:
		record=line.split()
		mediaw.append(record[0])##媒体相关词汇
##tstu=[1465536524]##待分类用户
others=[]##其他类用户
u_labels={}

##db=Connection().master_timeline_v1
##cl=db.master_timeline_user
##query_dict={'_id':u}
iter_users = s.iter_all_docs(fields=['_id','name','verified_type',
	'friends_count','followers_count','statuses_count','location','verified_reason','description'])
##for i in cl.find({'_id':u}):
##v_reason=i['verified_reason'].encode('utf-8')##获取认证原因
count=0
for r in iter_users:
	count+=1
	if count<=10:
		print datetime.datetime.now()
	if r['verified_type']==4:
		u_labels[str(r['_id'])+'_'+UPDATE_TIME]=labels[0]##高校微博
	elif r['verified_type']==1:
		if (r['location'].split()[0]).encode('utf-8') not in outlist:
			u_labels[str(r['_id'])+'_'+UPDATE_TIME]=labels[1]##境内机构
		else:
			u_labels[str(r['_id'])+'_'+UPDATE_TIME]=labels[2]##境外机构
	elif r['verified_type']==3:
		if (r['location'].split()[0]).encode('utf-8') not in outlist:
			u_labels[str(r['_id'])+'_'+UPDATE_TIME]=labels[3]##境内媒体
		else:
			u_labels[str(r['_id'])+'_'+UPDATE_TIME]=labels[4]##境外媒体 
	elif r['verified_type']==7:
		u_labels[str(r['_id'])+'_'+UPDATE_TIME]=labels[5]##民间组织
	elif r['verified_type']==0:
		kwdlst=[]
		text=r['name'].encode('utf-8')
		try:
			text=text+r['description'].encode('utf-8')
		except:
			noreason=noreason+1
		try:
			text=text+r['verified_reason']
		except:
			noreason=noreason+1
		##print v_reason
		s = load_scws()
		for word in s.participle(text):
			kwdlst.append(word[0])
		for w in kwdlst:
			if w in lawyerw:
				u_labels[str(r['_id'])+'_'+UPDATE_TIME]=labels[6]##律师
			elif w in adminw:
				u_labels[str(r['_id'])+'_'+UPDATE_TIME]=labels[7]##政府官员
			elif w in mediaw:
				u_labels[str(r['_id'])+'_'+UPDATE_TIME]=labels[8]##媒体人士
			else:
				u_labels[str(r['_id'])+'_'+UPDATE_TIME]=labels[9]##活跃人士
	else:
		if r['followers_count']>=FOLLOWER_THRE and r['statuses_count']>=STATUS_THRE:
			u_labels[str(r['_id'])+'_'+UPDATE_TIME]=labels[10]##草根
		else:
			u_labels[str(r['_id'])+'_'+UPDATE_TIME]=labels[11]##其他
			others.append(r['_id'])
	##print u_labels[str(r['_id'])+'_'+UPDATE_TIME]
	##print labels.index(u_labels[str(r['_id'])+'_'+UPDATE_TIME])
batch = leveldb.WriteBatch()
for k, v in u_labels.iteritems():
	batch.Put(k, v)
spieduser.Write(batch, sync=True)	
	##print u_labels[u]
	##print labels.index(u_labels[u])