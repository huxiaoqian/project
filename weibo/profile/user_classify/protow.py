#!/usr/bin/env python
#-*-coding:utf-8-*-
import pymongo
from getReadonlyDB import getReadonlyDB
from operator import itemgetter, attrgetter
import codecs 
import datetime
import time
import re
import urllib
import json
import sys
##import opencc

sys.path.append('../xapian_weibo')
from xapian_weibo.xapian_backend import XapianSearch

def readUidByArea(area):##read uids in each class
    uidset = set()  
    with open("./seedu/" + area + ".txt") as f2:
        for line in f2:
            uid = int(line.split()[0])
            uidset.add(uid)
    return uidset
    
def cinitialize(classes,dic,flag):##innitialize
    dic={}
    for c in classes:
        if flag==0:
            dic[c]=0
        else:
            if flag==1:
                dic[c]=[]
            else:
                if flag==2:
                    dic[c]={}
    return dic

if __name__ == '__main__':
    print 'start:'
    print  datetime.datetime.now()
    
    ####stopwords
    stpwds=open('stpwds.txt','r')
    swds=[]
    while True:
        line=stpwds.readline()
        if not line:
            break
        else:
            record=line.split()
            swds.append(record[0])

    classes=["education","culture", "fashion",'entertainment',"finance", "media", "sports", "technology"]
    ##cc = opencc.OpenCC('mix2s')##繁简体转换
   
    ####generate seed users
    ###period
    b=datetime.datetime(2012,10,1)
    tb=time.mktime(b.timetuple())
    e=datetime.datetime(2013,10,1)
    te=time.mktime(e.timetuple())
    s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline')##search by index
    s1 = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline')##search for original tweet
    total_uid_set = set()
    u_seed=[]##seed users
    u_cls={}##user and class mapping
    w_user={}##word segmentation
    n_s=0##number of seed users for each class

    sw_cls={}##seed user words statistics for each class
    sw_cls=cinitialize(classes,sw_cls,2)  
    sw={}##words from all seed users
    wp_cls={}##protowords for each class
    wp_cls=cinitialize(classes,wp_cls,1)
    Nswds=0
    exception=0
    for area in classes:
        n_s=0
        uidset = readUidByArea(area)
        for uid in uidset:
            total_uid_set.add(uid)
            u_cls[uid]=area
            n_s +=1
            if n_s>5:##seed users
                u_seed.append(uid)
                count, get_results = s.search(query={'user': uid,'timestamp': {'$gt': tb, '$lt':te}},sort_by=['timestamp'],fields=['user', 'terms','retweeted_status'])##search for seed users' words
                for r in get_results():
                    try:
                        for t in r['terms']:
                            ##st=cc.convert(t)##繁简体转换
                            if t not in swds:
                                if t not in sw_cls[area]:
                                    sw_cls[area][t]=r['terms'][t]
                                else:
                                    sw_cls[area][t]=r['terms'][t]+sw_cls[area][t]
                                if t not in sw:
                                    sw[t]=r['terms'][t]
                                else:
                                    sw[t]=r['terms'][t]+sw[t]
                            else:
                                Nswds+=1
                                continue

                        ###get originated tweets
                        if r['retweeted_status'] is not None:
                            rc, r_results = s1.search(query={'id': r['retweeted_status']},fields=['user', 'terms'])
                            for rr in r_results:
                                for t1 in rr['terms']:
                                    ##st1=cc.convert(t1)##繁简体转换
                                    if t1 not in swds:
                                        if t1 not in sw_cls[area]:
                                            sw_cls[area][t1]=r['terms'][t1]
                                        else:
                                            sw_cls[area][t1]=r['terms'][t1]+sw_cls[area][t1]
                                        if t1 not in sw:
                                            sw[t1]=r['terms'][t1]
                                        else:
                                            sw[t1]=r['terms'][t1]+sw[t1]
                                    else:
                                        Nswds+=1
                                        continue
                    except:
                        exception=exception+1
                        continue
            else:
                continue        
    ####sort pool words for each class
    wl_cls={}##proto words for each class
    wl_cls=cinitialize(classes,wl_cls,1)

    ###sort
    for area in classes:
        tmp_list=[]
        for w in sw_cls[area]:
            if (sw_cls[area])[w]>5:
                w_score=float((sw_cls[area])[w])/sw[w]
                pair=tuple([w,w_score])
                tmp_list.append(pair)
        wl_cls[area]=sorted(tmp_list,key=itemgetter(1),reverse=True)

    ###pick topN frequent use words as proto words
    topN=300
    pwl_cls={}##proto words for eacha class
    pwl_cls=cinitialize(classes,pwl_cls,1)
    for area in classes:
        nw=0##number of prototype words
        for w in wl_cls[area]:
            nw +=1
            if nw<=topN:
                pwl_cls[area].append(w)
            else:
                break
    ###save proto words
    wcls=open('protow.txt','w')##open('wcls.txt','w')
    outcount=0
    for area in classes:
        outcount +=1
        incount=0
        if outcount==1:
            wcls.write(str(area)+':')
            for w in pwl_cls[area]:
                incount +=1
                if incount==1:
                    wcls.write(w[0])
                else:
                    wcls.write(','+w[0])
        else:
            wcls.write('\n'+str(area)+':')
            for w in pwl_cls[area]:
                incount +=1
                if incount==1:
                    wcls.write(w[0])
                else:
                    wcls.write(','+w[0])
    print 'end:'
    print datetime.datetime.now()



