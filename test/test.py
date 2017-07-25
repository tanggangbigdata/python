import time
import sys
dt_time=time.time()
dt_day=time.strftime('%Y%m%d',time.localtime(time.time()))
print dt_day




def get_day(*args):
    dt_list=[]
    if len(args)==1:
     for i in range(0,8):
      dt=int(args[0])
      dt_time=int(time.mktime(time.strptime(dt,'%Y%m%d')))
      dt_tmp=time.strftime('%Y%m%d',time.localtime(dt_time-i*86400))
      dt_list.append(dt_tmp)
    return dt_list
    if len(args)==2:
      dt_time1= dt_time=int(time.mktime(time.strptime(args[0],'%Y%m%d')))
      dt_time2= dt_time=int(time.mktime(time.strptime(args[1],'%Y%m%d')))
      while dt_time1<dt_time2:
        for i in range(0,8):
          dt_tmp=time.strftime('%Y%m%d',time.localtime(dt_time-i*86400))
          dt_list.append(dt_tmp)
        dt_time1+=86400
    return dt_list

dt_list=get_day(20170620)
print dt_list
dt_list=get_day(20170615,20170610)
print dt_list
