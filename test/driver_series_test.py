#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-02"
#司机注册后的后续行为


from core.ychive import ychive
from core.dateTime import dateTime
from core.connmysql import connmysql
from core.queue import Td
import time
import datetime
import sys,os
import threading
import Queue
reload(sys)
sys.setdefaultencoding('utf-8')

class table:
  def execute(self,argv):
    dt,dty,start_time,end_time = dateTime().getDay(argv)
    while start_time<end_time:
       dt_today=time.strftime('%Y%m%d',time.localtime(int(time.time())-86400))
       start_time_1day_ago=start_time-86400
       start_time_1day_next=start_time+86400
       start_time_2day_next = start_time + 172800
       start_time_3day_next = start_time + 259200
       start_time_4day_next = start_time + 345600
       start_time_5day_next = start_time + 432000
       start_time_6day_next = start_time + 518400
       start_time_7day_next = start_time + 604800
       start_time_8day_next = start_time + 691200
       start_time_14day_next = start_time + 1123200
       start_time_30day_next = start_time + 2564800*8
       dt = time.strftime('%Y%m%d',time.localtime(start_time))
       dt2 = time.strftime('%Y%m%d',time.localtime(start_time_1day_next))
       dt3 = time.strftime('%Y%m%d', time.localtime(start_time_2day_next))
       dt4 = time.strftime('%Y%m%d', time.localtime(start_time_3day_next))
       dt5 = time.strftime('%Y%m%d', time.localtime(start_time_4day_next))
       dt6 = time.strftime('%Y%m%d', time.localtime(start_time_5day_next))
       dt7 = time.strftime('%Y%m%d', time.localtime(start_time_6day_next))
       dt8 = time.strftime('%Y%m%d', time.localtime(start_time_7day_next))
       dt14 = time.strftime('%Y%m%d', time.localtime(start_time_14day_next))
       dt30 = time.strftime('%Y%m%d', time.localtime(start_time_30day_next))


       sql="""insert overwrite table test.dm_driver_reg_series_detail_info_day_test partition(dt={dt})
       select t1.driver_id,from_unixtime(t1.create_time,'yyyyMMdd'),t2.driver_id,t2.dt,t3.driver_id,t3.dt,t4.driver_id,t4.dt
       from yc_dw_mp.dw_driver_reg_info t1
       left join yc_dw_mp.dw_dispatch_detail_driverid_unique_day t2
       on t1.driver_id=t2.driver_id and t2.dt>={dt} and t2.dt<{dt30}
       left join yc_dw_mp.dw_service_order_driverid_unique_day t3
       on t1.driver_id=t3.driver_id and t3.dt>={dt} and t3.dt<{dt30}
       left join yc_dw_mp.dw_service_order_driverid_unique_day t4
       on t1.driver_id=t4.driver_id and t4.status=7 and t4.dt>={dt} and t4.dt<{dt30}
       where t1.dt={dt_today} and from_unixtime(t1.create_time,'yyyyMMdd')={dt}
       """.format(dt_today=dt_today,dt=dt,dt2=dt2,dt3=dt3,dt4=dt4,dt5=dt5,dt6=dt6,dt7=dt7,dt8=dt8,dt14=dt14,dt30=dt30)
       ychive().query(sql)
       start_time+=86400

