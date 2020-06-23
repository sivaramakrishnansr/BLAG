from io import StringIO
import pandas as pd
from surprise import NMF,Reader,Dataset,model_selection
import json
from blag_support import parser, generate_prefix_data
import multiprocessing
import time
import glob
import argparse

class BLAG():
	def __init__(self, end_date, misclassifications_file, results_file):
		self.reference_end_time = parser(end_date)
		self.half_life_duration=30
		self.results_folder = results_file
		self.n_factors = 5
		self.epochs = 1100
		self.misclassifications_file = misclassifications_file


		self.all_ips_data = {}
		self.ip_16_data = {}
		self.misclassifications = {}

		self.init_blag_dataset()
		self.init_misclassifications()

	def init_blag_dataset(self):
		try:
			f=open("processed_blacklists","r")
		except:
			print ("processed_blacklists not found."
				"Run process_blacklist.py with Blacklist dataset.")
			exit(1)
		for line in f:
			ip=line.strip().split("qwerty123")[0]
			ip_16=".".join(ip.split(".")[0:3])+".0"
			value=json.loads(line.strip().split("qwerty123")[1])
			self.all_ips_data.setdefault(ip_16, {})
			self.ip_16_data.setdefault(ip_16, set())
			self.all_ips_data[ip_16][ip]=value
			self.ip_16_data[ip_16].add(ip)
			if len(self.all_ips_data)==100:
				break
		f.close()

	def init_misclassifications(self):
		f=open(self.misclassifications_file,"r")
		for line in f:
			ip=line.strip()
			ip_16=".".join(ip.split(".")[0:2])+".0.0"
			self.misclassifications.setdefault(ip_16,set())
			self.misclassifications[ip_16].add(ip)
		f.close()


	def listener(self, queue):
		fw=open(self.results_folder,"w")
		while True:
			data=queue.get()
			if data=="kill":
				break
			fw.write(data+"\n")
		fw.close()

	def run_process(self, all_ips_data, ip_16_data, misclassifications, queue):
		historical_item=generate_prefix_data(all_ips_data, ip_16_data,
		self.reference_end_time, self.half_life_duration)
		matrix=[]
		if len(historical_item)==0:
			return
		if len(historical_item)<5:
			for ip,bl_name_data in historical_item.items():
				queue.put(ip+",0")
			return


		matrix_string="userId,itemId,rating\n"
		all_blacklists=set()
		ip_order=set()
		for ip,bl_name_data in historical_item.items():
			ip_order.add(ip)
			for bl_name,score in bl_name_data.items():
				matrix_string=matrix_string+ip+","+bl_name+","+str(score)+"\n"
				all_blacklists.add(bl_name)
		for ip in misclassifications:
			if ip in ip_order:
				matrix_string=matrix_string+ip+","+"misclassifications,10"+"\n"


		matrix_string=StringIO(matrix_string)
		ratings = pd.read_csv(matrix_string)

		ratings_dict = {'itemID': list(ratings.itemId),
		'userID': list(ratings.userId),
		'rating': list(ratings.rating)}

		df = pd.DataFrame(ratings_dict)
		reader = Reader(rating_scale=(0, 10.0))
		data = Dataset.load_from_df(df[['userID', 'itemID', 'rating']], reader)
		epochs=100
		broken_flag=False
		while True:
			algo = NMF(n_epochs=epochs,n_factors=self.n_factors)
			try:
				res=model_selection.cross_validate(algo, data, measures=['RMSE'])
			except:
				broken_flag=True
				break
			mean_rmse=sum(res["test_rmse"])/len(res["test_rmse"])
			if mean_rmse<=1:
				break
			epochs=epochs+100
			if epochs>=self.epochs:
				break
		for ip in ip_order:
			prediction=algo.predict(ip, "misclassifications").est
			queue.put(ip+","+str(round(prediction,2)))
		return

	def generate_recommendations(self):
		jobs=[]
		manager = multiprocessing.Manager()
		queue = manager.Queue()
		cpus=int(multiprocessing.cpu_count()/2)
		pool = multiprocessing.Pool(cpus + 2)
		watcher = pool.apply_async(self.listener, (queue, ))
		for ip_16 in self.ip_16_data:
			self.misclassifications.setdefault(ip_16, {})
			self.run_process(self.all_ips_data[ip_16],
			self.ip_16_data[ip_16],
			self.misclassifications[ip_16], queue)
		queue.put("kill")
		watcher.get()

blag_args = argparse.ArgumentParser(description='Computing recommendation scores for IP address')
blag_args.add_argument('-e', '--end_date' , type=str,
                    help='End Date of processing', required = True)
blag_args.add_argument('-m', '--misclassifications', type=str,
                    help='File consisting of known missclassifications', required = True)
blag_args.add_argument('-o', '--output_file', type=str,
                    help='Results file', required = True)

args = blag_args.parse_args()
end_date, misclassifications_file, results_file = args.end_date, args.misclassifications, args.output_file

b = BLAG(end_date, misclassifications_file, results_file)
b.generate_recommendations()
