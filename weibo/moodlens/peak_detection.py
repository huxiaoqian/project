# -*- coding: utf-8 -*-
from __future__ import division
import numpy as np

def sentiment_variation(lis,cursor,dur=7):
	if dur < len(lis):                       				
		begin = cursor - int(dur/2)
		st = ''
		if begin < 0:
			begin = 0
			st += '0'
		end = begin + dur		
		if end > len(lis)-1:
                    end = len(lis)-1
                    begin = end-dur
                    st += '2'
                cursor = cursor-begin

	else:
            
		begin = 0
		end = len(lis)
		st = ''
	print begin,end,cursor,'seg begin,end,cursor'+st
	seg = lis[begin:end+1]
	ave = np.mean(seg)
	variation = [np.abs(ave-i)/ave for i in seg]
	ave = np.mean(variation)
	if variation[cursor] < ave:
		return 0
	else:
		return 1

def ema_list(lis,day):
	day = day+1
	ema = []
	for i in range(1,len(lis)):
		ema.append(lis[i-1]*(day-2)/day+lis[i]*2/day)
	ema.append(lis[-2]*(day-2)/day+lis[-1]*2/day)
	return ema

def dif_list(lis_s,lis_l):
	return [lis_s[i]-lis_l[i] for i in range(len(lis_s))]

def dea_lis(dif,day):
	day = day+1
	deas = [dif[0]]
	for i in range(1,len(dif)):
		deas.append(deas[i-1]*(day-2)/day+dif[i]*2/day)
	return deas
def MACD_list(dif,dea_lis):
	return [dif[i]-dea_lis[i] for i in range(len(dif))]

def find_topN(lis,n):
	new = [lis[0]]
	rank = [0]
	num_cursor = 1
	for num in lis[1:]:
		num_cursor += 1
		find = 0
		cursor = 0
		if num > new[0]:
			new[0:0] = [num]
			rank[0:0] = [num_cursor-1]
		else:
			for i in new:
				if num > i:
					new[cursor:cursor] = [num]
					rank[cursor:cursor] = [num_cursor-1]
					find = 1
					break
				cursor += 1
			if find == 0:
				new.append(num)
				rank.append(num_cursor-1)
			
	peak_x = []
	peak_y = []
	cursor = 0
	for y in new:
		if rank[cursor]!=0 and rank[cursor]!=len(new)-1:
			if y > lis[rank[cursor]+1] and y > lis[rank[cursor]-1]:
				peak_x.append(rank[cursor])
				peak_y.append(y)

		elif rank[cursor]==0:
			if y > lis[rank[cursor]+1]:
				peak_x.append(rank[cursor])
				peak_y.append(y)
		elif rank[cursor]==rank[cursor]!=len(new)-1:
			if y > lis[rank[cursor]+1]:
				peak_x.append(rank[cursor])
				peak_y.append(y)
		if len(peak_x)==n:
			break
		cursor += 1
	return peak_x[:n]

def detect_peaks(y1,topN=10):
	if len(y1) == 0:
		return []
	else:
		new_zeros = find_topN(y1,topN)
		print new_zeros,'step1 top'+str(topN)+'nodes'
		if y1[0] > y1[1]:
			new_zeros.append(0)
		if y1[-1] > y1[-2]:
			new_zeros.append(len(y1)-1)
		print new_zeros,'step2 edge nodes'
		if len(y1) >= 4:
			short_ema = ema_list(y1,12) ##计算12天的快速移动平均值
			long_ema = ema_list(y1,26)  ##计算26天的慢速移动平均值
			dif = dif_list(short_ema,long_ema)  ##根据 快速移动平均值 -慢速移动平均值 计算"差离值"（DIF）
			dea = dea_lis(dif,9) ##计算离差值 DIF的9日的移动平均值EM
			MACD = MACD_list(dif,dea) ##最后用DIFF减DEA，得MACD 若MACD符号发生变化，即DIF与DEA线发生交叉，则有拐点出现

			zeros = []
			for i in range(len(MACD)-1):
				if MACD[i] <= 0 and MACD[i+1] >= 0:
					zeros.append(i+1)

				elif MACD[i] > 0 and MACD[i+1] < 0:
					zeros.append(i+1)

			for zr in zeros:
				if zr != 0  and zr != len(y1)-1:
					if y1[zr]>y1[zr-1] and y1[zr]>y1[zr+1]:
						new_zeros.append(zr)
					for cursor in range(zr+1,zr+4):
						try:
							if y1[cursor]>=y1[cursor-1] and y1[cursor]>=y1[cursor+1]:
								new_zeros.append(cursor)
						except:
							continue
					for cursor in range(zr-4,zr-1):
						try:
							if y1[cursor]>=y1[cursor-1] and y1[cursor]>=y1[cursor+1]:
								new_zeros.append(cursor)
						except:
							continue
	new_zeros = set(new_zeros)
	new_zeros = list(new_zeros)
	new_zeros = sorted(new_zeros)
	print new_zeros,'step3 MACD nodes'
	cursor = -1
	filters = []
	for zr in new_zeros:                        
			cursor += 1
			if zr < 0 or zr > len(y1)-1:
				del new_zeros[cursor]
				filters.append(zr)
				continue
			vr = sentiment_variation(y1,zr)
			if vr == 0:
				del new_zeros[cursor]
				filters.append(zr)
	print filters,'filtered nodes'
	print new_zeros,'final nodes'
	return new_zeros,dif,dea
