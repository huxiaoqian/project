#!/usr/bin/env python
#-*-coding:utf-8-*-
from operator import itemgetter, attrgetter  
import pymongo
import datetime
from getReadonlyDB import getReadonlyDB
def readUidByArea(area):
    uidlist = []
    with open("./seedu/" + area + ".txt") as f:
        for line in f:
            uid = int(line.split()[0])
            uidlist.append(uid)
    return uidlist
    
def readFriendsByUid(uid):
    db_master_timeline = getReadonlyDB("master_timeline")
    cursor = db_master_timeline.master_timeline_user.find({"_id": uid})
    for user in cursor:
        if user['friends']!=[]:
            friends = user['friends']
            uid = user['_id']
            return {'_id': uid,'friends':friends}
if __name__ == '__main__':
    print 'start:'
    print datetime.datetime.now()
    classes = ["culture", "entertainment", "fashion",'education',"finance", "sports", "technology",'media']##8 class labels
    
    ####generate seed set
    seed={}##seed users
    pool={}##proto-user pool,get from seed users's friends
    su_cls={}##dictionary of seed user class
    n_s=0##number of seed users in each class
    foccur_all={}##total count of emerge
    friends=set()##all friends set of seed users
    error=0

    for area in classes:
        ###initialize
        su=set()##seed user for each class
        foccur_cls={}##count of emerge in current area
        n_s=0

        uidlist = readUidByArea(area)
        for uid in uidlist:
            n_s +=1###split for sample users
            if n_s>5:
                su.add(uid)
                dic=readFriendsByUid(uid)
                su_cls[uid]=area
                #print len(dic['friends'])
                try:
                    for f in dic['friends']:
                        friends.add(f)
                        if f in foccur_all:
                            foccur_all[f] +=1
                        else:
                            foccur_all[f]=1
                        if f in foccur_cls:
                            foccur_cls[f] +=1
                        else:
                            foccur_cls[f]=1
                except:
                    error +=1
                    continue
                    
            else:
                continue
        pool[area]=foccur_cls
        seed[area]=su
    print 'number of errors:%d'%error

    ####generate proto set
    
    ###sort pool users for each class
    s_cu={}
    for area in pool:
        lst=[]
        for f in pool[area]:
            c_count=pool[area][f]##count of f in current class
            a_count=foccur_all[f]##count of f in all
            score=(c_count*1.0)/a_count
            pair=tuple([f,score])
            lst.append(pair)
        s_cu[area]=sorted(lst,key=itemgetter(1),reverse=True)
    
    ###pick topN users from pool users and conbine with seed users as proto users for each class
    pu_cls={}
    topN=300

    for area in s_cu:
        u_count=0
        lst=set()
        for p in s_cu[area]:
            u_count +=1
            if u_count<=topN:
                lst.add(p[0])
                if p[0] not in su_cls:
                    su_cls[p[0]]=area
        for u in seed[area]:
            lst.add(u)
        pu_cls[area]=lst
        print area+str(len(s_cu[area]))##num of pool users
        print area+str(len(seed[area]))##num of seed users
        print area+str(len(pu_cls[area]))##num of proto users
    ###save proto users
    protou=open('protou1.txt','w')
    count=0
    for area in pu_cls:
        count +=1
        if count==1:
            incount=0
            protou.write(area+':')
            for u in pu_cls[area]:
                incount +=1
                if incount==1:
                    protou.write(str(u))
                else:
                    protou.write(' '+str(u))
        else:
            incount=0
            protou.write('\n'+area+':')
            for u in pu_cls[area]:
                incount +=1
                if incount==1:
                    protou.write(str(u))
                else:
                    protou.write(' '+str(u))
    print 'end:'
    print datetime.datetime.now()