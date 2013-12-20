# -*- coding: utf-8 -*-
from __future__ import division
import numpy as np
def min_variation(lis,dur=7,form=0):
	vs = []
	for cursor in range(len(lis)):
		if dur < len(lis):                       				
			begin = cursor - int(dur/2)
			if begin < 0:
				begin = 0
			end = begin + dur		
			if end > len(lis)-1:
	                    end = len(lis)-1
	                    begin = end-dur
		else:            
			begin = 0
			end = len(lis)-1
		seg = lis[begin:end+1]
		min_num = min(seg)
		if form == 0:
			vs.append((lis[cursor]-min_num))
		else:
			vs.append((lis[cursor]-min_num)/min_num)
	return vs

def filter_min_gap(lis,cursor,dur=7,form=1):
	vs = min_variation(lis,dur=dur,form=form)
	if vs[cursor] >= np.mean(vs):
		return 1
	else:
		return 0

def sentiment_variation(lis,cursor,dur=7):
	stds = []
	for i in range(len(lis)):
		if i+dur > len(lis):
			break
		seg = lis[i:i+dur]
		stds.append(np.std(seg))
	global_std = np.mean(stds)
	
	if dur < len(lis):                       				
		begin = cursor - int(dur/2)
		if begin < 0:
			begin = 0
		end = begin + dur		
		if end > len(lis)-1:
                    end = len(lis)-1
                    begin = end-dur
	else:            
		begin = 0
		end = len(lis)-1
	
	seg = lis[begin:end]

	ave = (seg[0]+seg[-1])/2
	local_std = np.std(seg)
	# print begin,end,cursor,seg,ave,ave*1.2,lis[cursor],global_std,local_std,'seg begin,end,cursor'+st	
	if lis[cursor]> 1.2*ave or local_std > 0.5*global_std:
		return 1
	else:
		return 0

def save_domain(lis,cursor,dur=11):
	if dur < len(lis):                       				
		begin = cursor - int(dur/2)
		st = ''
		if begin < 0:
			begin = 0
		end = begin + dur		
		if end > len(lis)-1:
                    end = len(lis)-1
                    begin = end-dur
	else:            
		begin = 0
		end = len(lis)-1

	seg = lis[begin:end+1]
	if lis[cursor] == max(seg):
		return 1
	else:
		return 0

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
			if y >= lis[rank[cursor]+1] and y >= lis[rank[cursor]-1]:
				peak_x.append(rank[cursor])

		elif rank[cursor]==0:
			if y >= lis[rank[cursor]+1]:
				peak_x.append(rank[cursor])

		elif rank[cursor]==len(new)-1:
			if y >= lis[rank[cursor]-1]:
				peak_x.append(rank[cursor])

		if len(peak_x)==n:
			break
		cursor += 1
	return peak_x[:n]

def filter_variation(lis,peaks):
	cursor = -1
	filters = []
	cursors = []
	for zr in peaks:
		cursor += 1
		if zr < 0 or zr > len(lis)-1:
			filters.append(zr)
			cursors.append(cursors)
			continue
		vr = sentiment_variation(lis,zr)
		if vr == 0:
			filters.append(zr)
			cursors.append(cursors)
	peaks = [peaks[i] for i in range(len(peaks)) if i not in cursors]

	return peaks,filters


def detect_peaks(lis,topN=10,form=1):
	if len(lis) ==[]:
		return []
	elif len(lis) == 1:
		return [0]
	else:
		peaks = find_topN(lis,topN)
		print peaks,'step1 top'+str(topN)+'nodes'
		if lis[0] > lis[1]:
			peaks.append(0)
		if lis[-1] > lis[-2]:
			peaks.append(len(lis)-1)
		print peaks,'step2 edge nodes'
	# peaks,filters = filter_variation(lis,peaks)
	# print filters,'filtered nodes'
	# print peaks,'step3 MACD nodes'
	
	remove_nodes = []
	for pk in peaks:
		remove_stay = [0,0]
		remove_stay[0] = filter_min_gap(lis,pk,dur=7,form=1)
		remove_stay[1] = save_domain(lis,pk,dur=13)
		print pk,remove_stay
		
		if remove_stay == [0,0]:
			remove_nodes.append(pk)
	print 'remove_nodes',remove_nodes
	new_zeros = set([pk for pk in peaks if pk not in remove_nodes])
	new_zeros = list(new_zeros)
	new_zeros = sorted(new_zeros)	
	print new_zeros,'final nodes'
	return new_zeros
