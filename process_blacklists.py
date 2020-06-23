from dateutil import parser
import time
import glob
import json
import os
import argparse

class BlacklistParser():
    def __init__(self, blacklist_folder, start_date, end_date, avoid_blacklist):
        self.output_file = "processed_blacklists"
        self.start_date = parser.parse(start_date)
        self.end_date = parser.parse(end_date)
        self.blacklist_folder = blacklist_folder
        self.temp_blacklist = {}
        self.files_within_range = []
        self.avoid_blacklists = []

        all_files = []
        all_years = glob.glob(blacklist_folder+"/*")
        for year in all_years:
        	all_months = glob.glob(year + "/*")
        	for month in all_months:
        		all_files = all_files + glob.glob(month + "/*")

        for file in all_files:
            date=parser.parse(file.split("/")[-1].split(".")[0])
            if date>=self.start_date and date<=self.end_date:
                if ".zip" in file:
                    self.files_within_range.append(file)

        self.files_within_range = sorted(self.files_within_range)
        if avoid_blacklist is not None:
            f=open(avoid_blacklist, "r")
            for line in f:
                self.avoid_blacklists.append(line.strip())
            f.close()

    def _process_blacklist_mapper(self, iteration):
        blacklist_mapper = {}
        iteration_file=iteration.split(".")[0]
        f = open (iteration_file+"/blacklist_mapper" , "r")
        for line in f:
            blacklist = line.strip().split(",")[0]
            count = line.strip().split(",")[1]
            blacklist_mapper[count] = blacklist
        f.close()
        return blacklist_mapper


    def _process_blacklist_file(self, blacklist_file_name, blacklist_mapper):
        iteration={}
        all_blacklists=set()
        f=open(blacklist_file_name , "r")
        for line in f:
            ip=line.strip().split(",")[0]
            for bl in line.strip().split(",")[1:]:
                bl = blacklist_mapper[bl]
                if bl in self.avoid_blacklists:
                    continue
                iteration.setdefault(bl, set())
                iteration[bl].add(ip)
                all_blacklists.add(bl)
        f.close()
        return iteration,all_blacklists

    def _write_output(self):
        file_to_write=open(self.output_file,"w")
        for key,value in self.temp_blacklist.items():
            try:
                file_to_write.write(key+"qwerty123"+json.dumps(value)+"\n")
            except:
                file_to_write.write(key+"qwerty123"+str(value)+"\n")
        file_to_write.close()


    def parse(self):
        iteration = self.files_within_range[0].split(".")[0]
        os.system("unzip -qq "+iteration+" -d "+"/".join(iteration.split("/")[:-1])+"/")
        blacklist_mapper = self._process_blacklist_mapper(iteration)
        start_time = iteration.split("/")[-1]
        blacklist_file_name = iteration + "/" + iteration.split("/")[-1]
        f=open(blacklist_file_name , "r")
        for line in f:
            ip=line.strip().split(",")[0]
            self.temp_blacklist.setdefault(ip, {})
            for bl in line.strip().split(",")[1:]:
                bl = blacklist_mapper[bl]
                self.temp_blacklist[ip].setdefault(bl, {})
                self.temp_blacklist[ip][bl]["Start Time"]=start_time
                self.temp_blacklist[ip][bl].setdefault("History","")
        f.close()

        for iteration in range(0,len(self.files_within_range)):
            if iteration+1 == len(self.files_within_range):
                break

            iteration_1 = self.files_within_range[iteration].split(".")[0]
            iteration_2 = self.files_within_range[iteration+1].split(".")[0]

            os.system("unzip -qq "+iteration_2+" -d "+"/".join(iteration_2.split("/")[:-1])+"/")

            all_blacklists = set()

            blacklist_mapper = self._process_blacklist_mapper(iteration_1)
            start_time_iteration_1 = iteration_1.split("/")[-1]
            blacklist_file_name = iteration_1 + "/" + iteration_1.split("/")[-1]
            iteration_1_bl, temp_all_blacklists = self._process_blacklist_file(blacklist_file_name, blacklist_mapper)
            all_blacklists.update(temp_all_blacklists)


            blacklist_mapper = self._process_blacklist_mapper(iteration_2)
            start_time_iteration_2 = iteration_2.split("/")[-1]
            blacklist_file_name = iteration_2 + "/" + iteration_2.split("/")[-1]
            iteration_2_bl, temp_all_blacklists = self._process_blacklist_file(blacklist_file_name, blacklist_mapper)
            all_blacklists.update(temp_all_blacklists)

            for bl in all_blacklists:
                iteration_1_bl.setdefault(bl, set())
                iteration_2_bl.setdefault(bl, set())

            add_ip_dict = {}
            remove_ip_dict = {}
            for bl in all_blacklists:
                for ip in iteration_1_bl[bl] - iteration_2_bl[bl]:
                    remove_ip_dict.setdefault(ip, {})
                    remove_ip_dict[ip].setdefault(bl, {})
                    remove_ip_dict[ip][bl]=start_time_iteration_2

                for ip in iteration_2_bl[bl] - iteration_1_bl[bl]:
                    add_ip_dict.setdefault(ip, {})
                    add_ip_dict[ip].setdefault(bl, {})
                    add_ip_dict[ip][bl]=start_time_iteration_1

            for ip,value in add_ip_dict.items():
                self.temp_blacklist.setdefault(ip, {})
                for bl, start_time in value.items():
                    self.temp_blacklist[ip].setdefault(bl, {})
                    self.temp_blacklist[ip][bl]["Start Time"]=start_time_iteration_1
                    self.temp_blacklist[ip][bl].setdefault("History","")

            for ip,value in remove_ip_dict.items():
                blacklist_dict=self.temp_blacklist[ip]
                for bl,bl_end_time in value.items():
                    bl_start_time=blacklist_dict[bl]["Start Time"]
                    blacklist_dict[bl]["History"]=blacklist_dict[bl]["History"]+"|"+bl_start_time+"     "+bl_end_time
                self.temp_blacklist[ip]=blacklist_dict

            print ("Iterations",iteration,iteration+1,"Temp blacklist",len(self.temp_blacklist),"Added",len(add_ip_dict),"Removed",len(remove_ip_dict))
            os.system("rm -rf "+iteration_1.split(".")[0])

        os.system("rm -rf "+iteration_2.split(".")[0])
        self._write_output()


blag_args = argparse.ArgumentParser(description='Process BLAG IR (intermiediete representation)')
blag_args.add_argument('-s', '--start_date' , type=str,
                    help='Start Date of processing', required = True)
blag_args.add_argument('-e', '--end_date' , type=str,
                    help='End Date of processing', required = True)
blag_args.add_argument('-b', '--blacklist_folder', type=str,
                    help='Blacklist folder containing blacklists', required = True)
blag_args.add_argument('-a', '--avoid_blacklist', type=str,
                    help='Avoid blacklists')

args = blag_args.parse_args()
start_date, end_date, blacklist_folder, avoid_blacklist= args.start_date, args.end_date, args.blacklist_folder, args.avoid_blacklist

bp = BlacklistParser(blacklist_folder, start_date, end_date, avoid_blacklist)
bp.parse()
