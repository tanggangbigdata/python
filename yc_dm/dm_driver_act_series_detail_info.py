#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-05"
#司机激活后的后续行为


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
       start_time_30day_next = start_time + 2564800
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
       sql="""insert overwrite table yc_dm_mp.dm_driver_act_series_detail_info_day partition(dt={dt})
       select t1.driver_id,
       t31.driver_id,t32.driver_id,t33.driver_id,t34.driver_id,t35.driver_id,t36.driver_id,t37.driver_id,t38.driver_id,t314.driver_id,t330.driver_id,
       t41.driver_id,t42.driver_id,t43.driver_id,t44.driver_id,t45.driver_id,t46.driver_id,t47.driver_id,t48.driver_id,t414.driver_id,t430.driver_id,
       t51.driver_id,t52.driver_id,t53.driver_id,t54.driver_id,t55.driver_id,t56.driver_id,t57.driver_id,t58.driver_id,t514.driver_id,t530.driver_id
       from yc_dm_mp.dm_driver_jihuo_all t1
       left join (select distinct(driver_id) from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt})t31
       on t1.driver_id=t31.driver_id
       left join (select distinct(driver_id) from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt2})t32
       on t1.driver_id=t32.driver_id 
       left join (select distinct(driver_id) from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt3})t33
       on t1.driver_id=t33.driver_id
       left join (select distinct(driver_id) from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt4})t34
       on t1.driver_id=t34.driver_id
       left join (select distinct(driver_id) from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt5})t35
       on t1.driver_id=t35.driver_id
       left join (select distinct(driver_id) from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt6})t36
       on t1.driver_id=t36.driver_id
       left join (select distinct(driver_id) from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt7})t37
       on t1.driver_id=t37.driver_id
       left join (select distinct(driver_id) from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt8})t38
       on t1.driver_id=t38.driver_id
       left join (select distinct(driver_id) from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt14})t314
       on t1.driver_id=t314.driver_id
       left join (select distinct(driver_id) from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt30})t330
       on t1.driver_id=t330.driver_id
       left join (select driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt})t41
       on t1.driver_id=t41.driver_id
       left join (select driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt2})t42
       on t1.driver_id=t42.driver_id
       left join (select driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt3})t43
       on t1.driver_id=t43.driver_id
       left join (select driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt4})t44
       on t1.driver_id=t44.driver_id
       left join (select driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt5})t45
       on t1.driver_id=t45.driver_id
       left join (select driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt6})t46
       on t1.driver_id=t46.driver_id
       left join (select driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt7})t47
       on t1.driver_id=t47.driver_id
       left join (select driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt8})t48
       on t1.driver_id=t48.driver_id
       left join (select driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt14})t414
       on t1.driver_id=t414.driver_id
       left join (select driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt30})t430
       on t1.driver_id=t430.driver_id
       left join(select driver_id from yc_dw_mp.dw_service_order_driverid_unique_day where dt={dt} and status=7)t51
       on t1.driver_id=t51.driver_id
       left join(select driver_id from yc_dw_mp.dw_service_order_driverid_unique_day where dt={dt2} and status=7)t52
       on t1.driver_id=t52.driver_id
       left join(select driver_id from yc_dw_mp.dw_service_order_driverid_unique_day where dt={dt3} and status=7)t53
       on t1.driver_id=t53.driver_id
       left join(select driver_id from yc_dw_mp.dw_service_order_driverid_unique_day where dt={dt4} and status=7)t54
       on t1.driver_id=t54.driver_id
       left join(select driver_id from yc_dw_mp.dw_service_order_driverid_unique_day where dt={dt5} and status=7)t55
       on t1.driver_id=t55.driver_id
       left join(select driver_id from yc_dw_mp.dw_service_order_driverid_unique_day where dt={dt6} and status=7)t56
       on t1.driver_id=t56.driver_id
       left join(select driver_id from yc_dw_mp.dw_service_order_driverid_unique_day where dt={dt7} and status=7)t57
       on t1.driver_id=t57.driver_id
       left join(select driver_id from yc_dw_mp.dw_service_order_driverid_unique_day where dt={dt8} and status=7)t58
       on t1.driver_id=t58.driver_id
       left join(select driver_id from yc_dw_mp.dw_service_order_driverid_unique_day where dt={dt14} and status=7)t514
       on t1.driver_id=t514.driver_id
       left join(select driver_id from yc_dw_mp.dw_service_order_driverid_unique_day where dt={dt30} and status=7)t530
       on t1.driver_id=t530.driver_id
       where from_unixtime(t1.active_time,'yyyyMMdd')={dt}
       """.format(dt=dt,dt2=dt2,dt3=dt3,dt4=dt4,dt5=dt5,dt6=dt6,dt7=dt7,dt8=dt8,dt14=dt14,dt30=dt30)
       print sql
       ychive().query(sql)
       start_time+=86400

