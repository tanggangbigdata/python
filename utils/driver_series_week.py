#!/bin/env python
import time
import sys
import os
import threading
from Queue import Queue
bi_list=[]
def get_week(*args):
  week_list=[]
  if len(args)==1:
   for i in range(0,11):
    dt=args[0]
    dt_time=int(time.mktime(time.strptime(str(dt),'%Y%m%d')))
    dt_tmp=time.strftime('%Y%m%d',time.localtime(dt_time-i*7*86400))
    week_list.append(dt_tmp)
   return week_list
  else:
    dt_time1= int(time.mktime(time.strptime(str(args[0]),'%Y%m%d')))
    dt_time2= int(time.mktime(time.strptime(str(args[1]),'%Y%m%d')))
    print dt_time1,dt_time2
    while dt_time1<dt_time2:
      for i in range(0,11):
        dt_tmp=time.strftime('%Y%m%d',time.localtime(dt_time1-i*7*86400))
        week_list.append(dt_tmp)
      dt_time1+=604800
    return week_list



dt_time=time.time()
dt_day=time.strftime('%Y%m%d',time.localtime(int(time.time())-86400))
print dt_day
reg_script_name="yc_dm.py dm_driver_reg_series_detail_week"
act_script_name="yc_dm.py dm_driver_act_series_detail_week"
bi_reg_script_name="yc_bi.py bi_dm_driver_reg_series_week"
bi_act_script_name="yc_bi.py bi_dm_driver_act_series_week"
#script_list=[bi_reg_script_name,bi_act_script_name]
script_list=[reg_script_name,act_script_name,bi_reg_script_name,bi_act_script_name]
path="/home/bigdata/hdata"
queue=Queue()
if len(sys.argv)==1:
    week_list=get_week(dt_day)
    print week_list
    for j in script_list:
        for i in week_list:
            command="python"+" "+j+" "+i
            queue.put(command)
elif len(sys.argv)==2:
    week_list=get_week(sys.argv[1])
    print week_list
    for j in script_list:
        for i in week_list:
            command="python"+" "+j+" "+i
            queue.put(command)
else:
    week_list=get_week(sys.argv[1],sys.argv[2])
    print week_list
    for j in script_list:
        for i in week_list:
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
