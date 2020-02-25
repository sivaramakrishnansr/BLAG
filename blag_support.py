import math

def range_check(s_time,e_time,timestamp):
        month_array=[0,31,28,31,30,31,30,31,31,30,31,30,31]
        timestamp=(int(timestamp.split("-")[0])*365)+(sum(month_array[:int(timestamp.split("-")[1])]))+int(timestamp.split("-")[-1])
        if e_time==" ":
                e_time=0
        else:
                e_time=(int(e_time.split("-")[0])*365)+(sum(month_array[:int(e_time.split("-")[1])]))+int(e_time.split("-")[-1])
        if s_time!="unknown":
                s_time=(int(s_time.split("-")[0])*365)+(sum(month_array[:int(s_time.split("-")[1])]))+int(s_time.split("-")[-1])
        if s_time=="unknown":
                if timestamp>=e_time:
                        delay=timestamp-e_time
                        return (delay,"early")
                delay=0
                return (delay,"on_time")
        if timestamp>=s_time and timestamp<=e_time:
                delay=0
                return (delay,"on_time")
        if timestamp>=e_time and e_time!=0:
                delay=timestamp-e_time
                return (delay,"early")
        if timestamp<=s_time:
                delay=s_time-timestamp
                return (delay,"late")
        if timestamp>=s_time and e_time==0:
                delay=0
                return (delay,"on_time")

def pre_parser(date):
        month_array=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        year=date.split("-")[0]
        month=month_array[int(date.split("-")[1])-1]
        day=date.split("-")[2]
        date=parser("Sun "+month+" "+day+" 22:10:02 UTC "+year)
        return date


def check_overlap(timeline_list):
	timeline_list=list(timeline_list)
	temp_timeline=set()
	done_set=set()
	for i in range(0,len(timeline_list)):
		for j in range(0,len(timeline_list)):
			if i==j:
					continue
			compare_1=timeline_list[i]
			compare_2=timeline_list[j]
			temp=[]
			temp.append(compare_1[0])
			temp.append(compare_1[1])
			temp.append(compare_2[0])
			temp.append(compare_2[0])
			temp=str(sorted(temp))
			if temp in done_set:
				continue
			done_set.add(str(temp))
			if compare_2[1] >= compare_1[0] and compare_2[1] <=compare_1[1]:
					continue
			if compare_1[1] >= compare_2[0] and compare_1[1] <=compare_2[1]:
					continue
			temp_timeline.add(compare_1)
			temp_timeline.add(compare_2)

	return len(temp_timeline)

def half_life(days_before,h):
	return math.pow(2,(-days_before/float(h)))

def parser(date_string):
	month_array=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
	month_dict=[31,28,31,30,31,30,31,31,30,31,30,31]
	date_string=" ".join(date_string.split()).replace("UTC","")
	date_string=date_string.strip().split()
	year=int(date_string[-1])
	month=date_string[1]
	date=int(date_string[2])
	year_sum=(year-1)*365
	month_sub_array=month_dict[0:month_array.index(month)]
	month_sum=sum(month_sub_array)
	date_sum=date
	sum_1=year_sum+month_sum+date_sum
	return sum_1
