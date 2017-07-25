#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-08"
#司机激活后的后续月行为


from core.ychive import ychive
from core.dateTime import dateTime
from core.connmysql import connmysql
from core.queue import Td
from dm_driver_reg_series_detail_month import dt_9list
import time
import datetime
import calendar
import sys,os
import threading
import Queue
reload(sys)
sys.setdefaultencoding('utf-8')

class table:
  def execute(self,argv):
    dt,dty,start_time,end_time = dateTime().getDay(argv)
    while start_time<end_time:
       dt_list = dt_9list(start_time + 86400)
       print dt_list
       dt_mon1 = dt_list[0]
       dt_mon2 = dt_list[1]
       dt_mon3 = dt_list[2]
       dt_mon4 = dt_list[3]
       dt_mon5 = dt_list[4]
       dt_mon6 = dt_list[5]
       dt_mon7 = dt_list[6]
       dt_mon8 = dt_list[7]
       dt_mon9 = dt_list[8]
       sql="""insert overwrite table yc_dm_mp.dm_driver_act_series_detail_info_mon partition(dt={dt_mon1})
       select t1.driver_id,
       t31.driver_id,t32.driver_id,t33.driver_id,t34.driver_id,t35.driver_id,t36.driver_id,t37.driver_id,t38.driver_id,
       t41.driver_id,t42.driver_id,t43.driver_id,t44.driver_id,t45.driver_id,t46.driver_id,t47.driver_id,t48.driver_id,
       t51.driver_id,t52.driver_id,t53.driver_id,t54.driver_id,t55.driver_id,t56.driver_id,t57.driver_id,t58.driver_id
       from yc_dm_mp.dm_driver_jihuo_all t1
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt_mon1} and dt<{dt_mon2})t31
       on t1.driver_id=t31.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt_mon2} and dt<{dt_mon3})t32
       on t1.driver_id=t32.driver_id 
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt_mon3} and dt<{dt_mon4})t33
       on t1.driver_id=t33.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt_mon4} and dt<{dt_mon5})t34
       on t1.driver_id=t34.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt_mon5} and dt<{dt_mon6})t35
       on t1.driver_id=t35.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt_mon6} and dt<{dt_mon7})t36
       on t1.driver_id=t36.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt_mon7} and dt<{dt_mon8})t37
       on t1.driver_id=t37.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt_mon8} and dt<{dt_mon9})t38
       on t1.driver_id=t38.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt_mon1} and dt<{dt_mon2})t41
       on t1.driver_id=t41.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt_mon2} and dt<{dt_mon3})t42
       on t1.driver_id=t42.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt_mon3} and dt<{dt_mon4})t43
       on t1.driver_id=t43.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt_mon4} and dt<{dt_mon5})t44
       on t1.driver_id=t44.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt_mon5} and dt<{dt_mon6})t45
       on t1.driver_id=t45.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt_mon6} and dt<{dt_mon7})t46
       on t1.driver_id=t46.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt_mon7} and dt<{dt_mon8})t47
       on t1.driver_id=t47.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt_mon8} and dt<{dt_mon9})t48
       on t1.driver_id=t48.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where dt>={dt_mon1} and dt<{dt_mon2} and status=7)t51
       on t1.driver_id=t51.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where dt>={dt_mon2} and dt<{dt_mon3} and status=7)t52
       on t1.driver_id=t52.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where dt>={dt_mon3} and dt<{dt_mon4} and status=7)t53
       on t1.driver_id=t53.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where dt>={dt_mon4} and dt<{dt_mon5} and status=7)t54
       on t1.driver_id=t54.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where dt>={dt_mon5} and dt<{dt_mon6} and status=7)t55
       on t1.driver_id=t55.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where dt>={dt_mon6} and dt<{dt_mon7} and status=7)t56
       on t1.driver_id=t56.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where dt>={dt_mon7} and dt<{dt_mon8} and status=7)t57
       on t1.driver_id=t57.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where dt>={dt_mon8} and dt<{dt_mon9} and status=7)t58
       on t1.driver_id=t58.driver_id 
       where from_unixtime(t1.active_time,'yyyyMMdd')>={dt_mon1} and from_unixtime(t1.active_time,'yyyyMMdd')<{dt_mon2}
       """.format(dt_mon1=dt_mon1,dt_mon2=dt_mon2,dt_mon3=dt_mon3,dt_mon4=dt_mon4,dt_mon5=dt_mon5,dt_mon6=dt_mon6,
                  dt_mon7=dt_mon7,dt_mon8=dt_mon8,dt_mon9=dt_mon9)
       print sql
       ychive().query(sql)
       start_time += 86400







