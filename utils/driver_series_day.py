#!/bin/env python
import time
import sys
import os
import threading
from Queue import Queue
import logging

bi_list=[]
def get_day(*args):
  dt_list=[]
  if len(args)==1:
   for i in range(0,8):
    dt=args[0]
    dt_time=int(time.mktime(time.strptime(str(dt),'%Y%m%d')))
    dt_tmp=time.strftime('%Y%m%d',time.localtime(dt_time-i*86400))
    dt_list.append(dt_tmp)
   dt_list.append(time.strftime('%Y%m%d',time.localtime(dt_time-13*86400)))
   dt_list.append(time.strftime('%Y%m%d', time.localtime(dt_time - 29 * 86400)))
   return dt_list
  else:
    dt_time1= int(time.mktime(time.strptime(str(args[0]),'%Y%m%d')))
    dt_time2= int(time.mktime(time.strptime(str(args[1]),'%Y%m%d')))
    print dt_time1,dt_time2
    while dt_time1<dt_time2:
      for i in range(0,8):
        dt_tmp=time.strftime('%Y%m%d',time.localtime(dt_time1-i*86400))
        dt_list.append(dt_tmp)
      dt_list.append(time.strftime('%Y%m%d',time.localtime(dt_time1-13*86400)))
      dt_list.append(time.strftime('%Y%m%d',time.localtime(dt_time1-29*86400)))
      dt_time1+=86400
    return dt_list



dt_time=time.time()
dt_day=time.strftime('%Y%m%d',time.localtime(int(time.time())-86400))
reg_script_name="yc_dm.py dm_driver_reg_series_detail_info"
act_script_name="yc_dm.py dm_driver_act_series_detail_info"
bi_reg_script_name="yc_bi.py bi_dm_driver_reg_series_day"
bi_act_script_name="yc_bi.py bi_dm_driver_act_series_day"
script_list=[reg_script_name,act_script_name,bi_reg_script_name,bi_act_script_name]
#script_list=[bi_reg_script_name,bi_act_script_name]
path="/home/bigdata/hdata"
queue=Queue()
queue2=Queue()
print dt_day


if len(sys.argv)==1:
    dt_list=get_day(dt_day)
    print dt_list
    for j in script_list:
        for i in dt_list:
            command="python"+" "+j+" "+i
            queue.put(command)

elif len(sys.argv)==2:
    dt_list=get_day(sys.argv[1])
    print dt_list
    for j in script_list:
        for i in dt_list:
            command="python"+" "+j+" "+i
            queue.put(command)
else:
    dt_list=get_day(sys.argv[1],sys.argv[2])
    print dt_list
    for j in script_list:
        for i in dt_list:
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
