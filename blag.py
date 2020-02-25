from io import StringIO
import pandas as pd
import sys
import numpy as np
from surprise import NMF,Reader,Dataset,model_selection
import json
from generate_prefix_data import generate_prefix_data
from blag_support import parser,send_mail
import multiprocessing
import time
import glob
import os

def convert_date(date):
	month_dict=[31,28,31,30,31,30,31,31,30,31,30,31]
	year_sum=(int(date.split("-")[0])-1)*365
	month_sub_array=month_dict[0:int(date.split("-")[1])-1]
	month_sum=sum(month_sub_array)
	date_sum=int(date.split("-")[-1])
	return year_sum+month_sum+date_sum



def run_process(ip_16,reference_end_time,date,misclassification_blacklist,avoid_blacklists,results_folder,half_life_duration_days,processed_ips_files,k_value):
	historical_item=generate_prefix_data(ip_16,reference_end_time,processed_ips_files,avoid_blacklists,half_life_duration_days)
	fw=open(results_folder+ip_16,"w")
	matrix=[]
	if len(historical_item)==0:
		return
	if len(historical_item)<5:
		for ip,bl_name_data in historical_item.items():
			on_day_blacklists="|".join(on_day_items[ip])
			all_scores=[]
			for bl_name,score in bl_name_data.items():
				all_scores.append(score)
			on_day_bl=""
			if ip in on_day_items:
				on_day_bl="|".join(list(on_day_items[ip]))
			all_day_bl=""
			if ip in all_day_items:
				all_day_bl="|".join(list(all_day_items[ip]))

			fw.write(ip+",0"+","+on_day_bl+","+all_day_bl+"\n")
		fw.close()
		return


	matrix_string="userId,itemId,rating\n"

	all_blacklists=set()

	ip_order=set()
	for ip,bl_name_data in historical_item.items():
		ip_order.add(ip)
		for bl_name,score in bl_name_data.items():
			matrix_string=matrix_string+ip+","+bl_name+","+str(score)+"\n"
			all_blacklists.add(bl_name)

	#Allocating a score of 1 to misclassification blacklists
	for ip in misclassification_blacklist:
		if ip in ip_order:
			matrix_string=matrix_string+ip+","+"false_positives,1"+"\n"

	matrix_string=StringIO(matrix_string)
	ratings = pd.read_csv(matrix_string)
	ratings_dict = {'itemID': list(ratings.itemId),'userID': list(ratings.userId),'rating': list(ratings.rating)}
	df = pd.DataFrame(ratings_dict)
	reader = Reader(rating_scale=(0, 10.0))
	data = Dataset.load_from_df(df[['userID', 'itemID', 'rating']], reader)
	epochs=100
	broken_flag=False
	while True:
		algo = NMF(n_epochs=epochs,n_factors=k_value)
		try:
			res=model_selection.cross_validate(algo, data, measures=['RMSE'])
		except:
			broken_flag=True
			break
		mean_rmse=sum(res["test_rmse"])/len(res["test_rmse"])
		if mean_rmse<=1:
			break
		epochs=epochs+100
		if epochs>=1100:
			break

	if broken_flag==True:
		for ip,bl_name_data in historical_item.items():
			on_day_blacklists="|".join(on_day_items[ip])
			all_scores=[]
			for bl_name,score in bl_name_data.items():
				all_scores.append(score)
			on_day_bl=""
			if ip in on_day_items:
				on_day_bl="|".join(list(on_day_items[ip]))
			all_day_bl=""
			if ip in all_day_items:
				all_day_bl="|".join(list(all_day_items[ip]))
			fw.write(ip+",0"+","+on_day_bl+","+all_day_bl+"\n")
		fw.close()
		return

	all_count=0
	other_count=0
	for ip in ip_order:
		prediction=algo.predict(ip, "false_positives").est
		on_day_bl=""
		if ip in on_day_items:
			on_day_bl="|".join(list(on_day_items[ip]))
		all_day_bl=""
		if ip in all_day_items:
			all_day_bl="|".join(list(all_day_items[ip]))

		fw.write(ip+","+str(round(prediction,2))+","+on_day_bl+","+all_day_bl+"\n")
	fw.close()
	return

def generate_pd(processed_ips_files,known_legitimate_senders,half_life_duration_days,k_value,results_folder,avoid_blacklists):
	known_false_posiitves={}
	f=open(known_legitimate_senders,"r")
	for line in f:
		ip=line.strip()
		ip_16=".".join(ip.split(".")[0:2])+".0.0"
		if ip_16 not in false_positives:
			known_false_positives[ip_16]=set()
		known_false_positives[ip_16].add(ip)
	f.close()

	os.system("mkdir -p "+results_folder)

	jobs=[]
	manager = multiprocessing.Manager()
	cpus=int(multiprocessing.cpu_count()/2)
	pool = multiprocessing.Pool(cpus + 2)

	ip_16_set=set()
	f=open(bl_file,"r")
	for line in f:
		ip=line.strip().split("qwerty123")[0]
		ip_16=".".join(ip.split(".")[0:3])+".0"
		ip_16_set.add(ip_16)
	f.close()

	total=len(ip_16_set)
	done_count=0
	done_creations_date=set()
	for item in ip_16_set:
		ip_16=item.split("|")[0]
		date=item.split("|")[1]
		date_sum=convert_date(item.split("|")[1])
		try:
			misclassification_blacklist=known_false_positives[ip_16]
		except:
			misclassification_blacklist={}
		if date not in done_creations_date:
			os.system("mkdir "+results_folder+"/"+date+"/ 2>/dev/null")
			done_creations_date.add(date)
		job = pool.apply_async(run_process, (ip_16,date_sum,date,misclassification_blacklist,avoid_blacklists,results_folder,half_life_duration_days,k_value,))
		jobs.append(job)
	for job in jobs:
		job.get()
		done_count=done_count+1
		print (done_count,total)
	pool.close()


config_file=sys.argv[1]
f=open(config_file,"r")
for line in f:
	if "#" in line:
		continue
	if "known_legitimate_senders" in line:
		known_legitimate_senders=line.strip().split("=")[1]
	if "processed_ips_files" in line:
		processed_ips_files=line.strip().split("=")[1]
	if "l=" in line:
		half_life_duration_days=int(line.strip().split("=")[1])
	if "K=" in line:
		k_value=int(line.strip().split("=")[1])
	if "results_folder" in line:
		results_folder=line.strip().split("=")[1]
	if "avoid_blacklists" in line:
		avoid_blacklists=line.strip().split("=")[1]


bl_file=sys.argv[1]
generate_pd(processed_ips_files,known_legitimate_senders,half_life_duration_days,k_value,results_folder,avoid_blacklists)
