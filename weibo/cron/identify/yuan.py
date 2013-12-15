#-*- coding: utf-8 -*-

from whole import followers_rank, active_rank, important_rank
from utils import read_rank_results
#from daily_user_important2leveldb import make as important_make, read as important_read

def yuan():
    data = important_rank(2000,'2013-9-30',1)
##    data = read_rank_results(2000,'whole','active','2013-9-30',1)
    print len(data)
if __name__ == '__main__':
    yuan()
