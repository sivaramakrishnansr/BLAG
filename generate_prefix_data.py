import os
import glob
import json
import time
import numpy as np
from blacklist_support import check_overlap,half_life,parser

def generate_prefix_data(current_prefix,reference_end_time,output_folder,avoid_blacklists,half_life_duration):
	#Reading data from raw file
	ip_16_dict={}
	ip_16_dict[current_prefix]=set()
	all_ips_data={}

	f=open(output_folder+"/"+current_prefix,"r")
	for line in f:
		try:
			ip=line.strip().split("qwerty123")[0]
			value=json.loads(line.strip().split("qwerty123")[1])
		except:
			continue
		all_ips_data[ip]=value
		ip_16_dict[current_prefix].add(ip)
	f.close()


	all_blacklists=set()
	for ip in ip_16_dict[current_prefix]:
		for blacklist,_ in all_ips_data[ip]["Blacklist"].items():
			blacklist=blacklist.split(".")[0]
			if blacklist in avoid_blacklists:
				all_blacklists.add(blacklist)
	all_blacklists=sorted(list(all_blacklists))

	blacklist_array=[]
	historical_return_items={}
	for ip in ip_16_dict[current_prefix]:
		ip_24=".".join(ip.split(".")[0:3])+".0"
		temp_array_exp1=[0]*len(all_blacklists)
		all_late=0
		included_blacklist=set()
		for blacklist,blacklist_data in all_ips_data[ip]["Blacklist"].items():
			blacklist=blacklist.split(".")[0].strip()
			if blacklist not in avoid_blacklists:
				continue
			start=blacklist_data["Start Time"]
			last_end_time=parser("Sun Jan 5 22:10:02 UTC 2002")
			last_end_string=""
			start_included=False
			unknown_flag=False
			length_of_timeline=len(blacklist_data["History"].split("|")[1:])
			for timeline in blacklist_data["History"].split("|")[1:]:
				if start in timeline:
					start_included=True
				if "unknown" in timeline:
					unknown_flag=True
					break
				start_time=parser(timeline.strip().split("    ")[0].strip())
				end_time=parser(timeline.strip().split("    ")[1].strip())
				if end_time > last_end_time:
					last_end_time=end_time
					last_end_string=timeline.strip().split("    ")[1].strip()
			length_of_timeline=len(blacklist_data["History"].split("|")[1:])
			if unknown_flag==True:
				continue
			delay=reference_end_time-last_end_time
			if start_included==False:
				start=parser(start)
				if start<=reference_end_time:
					delay=0
			if delay<0:
				continue

			historical_score=round(half_life(delay,half_life_duration),2)
			if ip not in historical_return_items:
				historical_return_items[ip]={}
			historical_return_items[ip][blacklist]=historical_score

	return historical_return_items
