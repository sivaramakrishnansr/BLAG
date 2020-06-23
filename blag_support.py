import math

def half_life(days_before,h):
	return math.pow(2,(-days_before/float(h)))

def parser(date_string):
    date_string = date_string.split("-")
    year=int(date_string[0])
    month=int(date_string[1])
    date=int(date_string[2])
    month_dict=[31,28,31,30,31,30,31,31,30,31,30,31]
    year_sum=(year-1)*365
    month_sub_array=month_dict[0:month]
    month_sum=sum(month_sub_array)
    sum_1=year_sum+month_sum+date
    return sum_1


def generate_prefix_data(all_ips_data, ip_16_dict,reference_end_time,half_life_duration):
	all_blacklists=set()
	for ip in ip_16_dict:
		for blacklist,_ in all_ips_data[ip].items():
			blacklist=blacklist.split(".")[0]
	all_blacklists=sorted(list(all_blacklists))

	blacklist_array=[]
	historical_return_items={}
	for ip in ip_16_dict:
		ip_24=".".join(ip.split(".")[0:3])+".0"
		temp_array_exp1=[0]*len(all_blacklists)
		for blacklist,blacklist_data in all_ips_data[ip].items():
			blacklist=blacklist.split(".")[0].strip()
			start=blacklist_data["Start Time"]
			last_end_time=parser("2002-01-05")
			last_end_string=""
			start_included=False
			on_time_flag=False
			length_of_timeline=len(blacklist_data["History"].split("|")[1:])
			for timeline in blacklist_data["History"].split("|")[1:]:
				if start in timeline:
					start_included=True
				start_time=parser(timeline.strip().split("    ")[0].strip())
				end_time=parser(timeline.strip().split("    ")[1].strip())
				if end_time > last_end_time:
					last_end_time=end_time
					last_end_string=timeline.strip().split("    ")[1].strip()
				if reference_end_time>=start_time and reference_end_time<=end_time:
					on_time_flag=True

			delay=reference_end_time-last_end_time
			if start_included==False:
				start=parser(start)
				if start<=reference_end_time:
					delay=0
			if delay<0:
				continue
			if on_time_flag==True:
				delay=0

			historical_score=round(half_life(delay,half_life_duration)*10,2)
			if ip not in historical_return_items:
				historical_return_items[ip]={}
			historical_return_items[ip][blacklist]=historical_score
	return historical_return_items
