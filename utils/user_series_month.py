#!/bin/env python
import time
import sys
import calendar
import os
import threading
from Queue import Queue

bi_list=[]
def get_mon(*args):
  dt_mon_list=[]
  if len(args)==1:
   dt_mon_list=dt_9list(int(time.mktime(time.strptime(str(args[0]),'%Y%m%d'))))
   return dt_mon_list
  else:
    dt_time1=int(time.mktime(time.strptime(str(args[0]),'%Y%m%d')))
    dt_time2=int(time.mktime(time.strptime(str(args[1]),'%Y%m%d')))
    print dt_time1,dt_time2
    while dt_time1<dt_time2:
      dt_mon_list=dt_9list(int(time.mktime(time.strptime(str(args[0]),'%Y%m%d'))))
      dt_time1+=86400*30
    return dt_mon_list

def dt_9list(start_time):
    dt_list = []
    first_time = start_time
    dt = time.strftime('%Y%m%d', time.localtime(first_time))
    dt_cur_mon_day = dt
    dt_list.append(dt)
    for i in range(1, 9):
        dt_cur_mon_day = time.strftime('%Y%m%d', time.localtime(first_time - 86400))
        monthRange = calendar.monthrange(int(dt_cur_mon_day[0:4]), int(dt_cur_mon_day[4:6]))
        monday = monthRange[1]
        print monday
        timerange = int(int(monday) * 86400)
        print timerange
        first_time = int(first_time) - timerange
        dt = time.strftime('%Y%m%d', time.localtime(first_time))
        dt_cur_mon_day = dt
        dt_list.append(dt)
    return dt_list

dt_time=time.time()
dt_day=time.strftime('%Y%m%d',time.localtime(int(time.time())-86400))
reg_script_name="yc_dm.py dm_user_reg_series_detail_month"
act_script_name="yc_dm.py dm_user_act_series_detail_month"
charge_script_name="yc_dm.py dm_user_charge_series_detail_month"
bi_reg_script_name="yc_bi.py bi_dm_user_reg_series_month"
bi_act_script_name="yc_bi.py bi_dm_user_act_series_month"
charge_script_name="yc_bi.py bi_dm_user_charge_series_month"
script_list=[reg_script_name,act_script_name,charge_script_name,bi_reg_script_name,bi_act_script_name,charge_script_name]
queue=Queue()
path="/home/bigdata/hdata"
print dt_day
if len(sys.argv)==1:
    mon_list=get_mon(dt_day)
    print mon_list
    for j in script_list:
        for i in mon_list:
            command="python"+" "+j+" "+i
            queue.put(command)
elif len(sys.argv)==2:
    mon_list=get_mon(sys.argv[1])
    print mon_list
    for j in script_list:
        for i in mon_list:
            command="python"+" "+j+" "+i
            queue.put(command)
else:
    mon_list=get_mon(sys.argv[1],sys.argv[2])
    print mon_list
    for j in script_list:
        for i in mon_list:
            command="python"+" "+j+" "+i
            queue.put(command)

class run_list((threading.Thread)):
    def __init__(self, threadID, queue):
        threading.Thread.__init__(self)
        self.thredID = threadID
        self.queue = queue
    def run(self):
        os.chdir(path)
        while True:
            try:
                command=queue.get()
                if not command.split(" ")[2].startswith("bi"):
                    print command
                    os.system(command)
                else:
                    bi_list.append(command)
            finally:
                self.queue.task_done()


for i in script_list:
    t=run_list(i,queue)
    t.setDaemon(True)
    t.start()
queue.join()
for i in bi_list:
    os.chdir(path)
    os.system(i)