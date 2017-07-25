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
       dt_today = time.strftime('%Y%m%d', time.localtime(int(time.time()) - 86400))
       start_time_1week_mon_next = start_time
       start_time_1week_sat_next = start_time + 518400
       start_time_2week_mon_next = start_time + 604800
       start_time_2week_sat_next = start_time + 1206000
       start_time_3week_mon_next = start_time + 1209600
       start_time_3week_sat_next = start_time + 1728000
       start_time_4week_mon_next = start_time + 1814400
       start_time_4week_sat_next = start_time + 2332800
       start_time_5week_mon_next = start_time + 2419200
       start_time_5week_sat_next = start_time + 2937600
       start_time_6week_mon_next = start_time + 3024000
       start_time_6week_sat_next = start_time + 3542400
       start_time_7week_mon_next = start_time + 3628800
       start_time_7week_sat_next = start_time + 4147200
       start_time_8week_mon_next = start_time + 4233600
       start_time_8week_sat_next = start_time + 4752000
       start_time_9week_mon_next = start_time + 4838400
       start_time_9week_sat_next = start_time + 5356800
       start_time_10week_mon_next = start_time + 5443200
       start_time_10week_sat_next = start_time + 5961600
       dt_1_mon = time.strftime('%Y%m%d', time.localtime(start_time_1week_mon_next))
       dt_1_sta = time.strftime('%Y%m%d', time.localtime(start_time_1week_sat_next))
       dt_2_mon = time.strftime('%Y%m%d', time.localtime(start_time_2week_mon_next))
       dt_2_sta = time.strftime('%Y%m%d', time.localtime(start_time_2week_sat_next))
       dt_3_mon = time.strftime('%Y%m%d', time.localtime(start_time_3week_mon_next))
       dt_3_sta = time.strftime('%Y%m%d', time.localtime(start_time_3week_sat_next))
       dt_4_mon = time.strftime('%Y%m%d', time.localtime(start_time_4week_mon_next))
       dt_4_sta = time.strftime('%Y%m%d', time.localtime(start_time_4week_sat_next))
       dt_5_mon = time.strftime('%Y%m%d', time.localtime(start_time_5week_mon_next))
       dt_5_sta = time.strftime('%Y%m%d', time.localtime(start_time_5week_sat_next))
       dt_6_mon = time.strftime('%Y%m%d', time.localtime(start_time_6week_mon_next))
       dt_6_sta = time.strftime('%Y%m%d', time.localtime(start_time_6week_sat_next))
       dt_7_mon = time.strftime('%Y%m%d', time.localtime(start_time_7week_mon_next))
       dt_7_sta = time.strftime('%Y%m%d', time.localtime(start_time_7week_sat_next))
       dt_8_mon = time.strftime('%Y%m%d', time.localtime(start_time_8week_mon_next))
       dt_8_sta = time.strftime('%Y%m%d', time.localtime(start_time_8week_sat_next))
       dt_9_mon = time.strftime('%Y%m%d', time.localtime(start_time_9week_mon_next))
       dt_9_sta = time.strftime('%Y%m%d', time.localtime(start_time_9week_sat_next))
       dt_10_mon = time.strftime('%Y%m%d', time.localtime(start_time_10week_mon_next))
       dt_10_sta = time.strftime('%Y%m%d', time.localtime(start_time_10week_sat_next))
       sql="""insert overwrite table yc_dm_mp.dm_driver_reg_series_detail_info_week partition(dt={dt11})
       select t1.driver_id,
       t21.driver_id,t22.driver_id,t23.driver_id,t24.driver_id,t25.driver_id,t26.driver_id,t27.driver_id,t28.driver_id,t214.driver_id,t230.driver_id,
       t31.driver_id,t32.driver_id,t33.driver_id,t34.driver_id,t35.driver_id,t36.driver_id,t37.driver_id,t38.driver_id,t314.driver_id,t330.driver_id,
       t41.driver_id,t42.driver_id,t43.driver_id,t44.driver_id,t45.driver_id,t46.driver_id,t47.driver_id,t48.driver_id,t414.driver_id,t430.driver_id,
       t51.driver_id,t52.driver_id,t53.driver_id,t54.driver_id,t55.driver_id,t56.driver_id,t57.driver_id,t58.driver_id,t514.driver_id,t530.driver_id
       from yc_dw_mp.dw_driver_reg_info t1
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt11} and from_unixtime(active_time,'yyyyMMdd')<={dt17})t21
       on t1.driver_id=t21.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt21} and from_unixtime(active_time,'yyyyMMdd')<={dt27})t22
       on t1.driver_id=t22.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt31} and from_unixtime(active_time,'yyyyMMdd')<={dt37})t23
       on t1.driver_id=t23.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt41} and from_unixtime(active_time,'yyyyMMdd')<={dt47})t24
       on t1.driver_id=t24.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt51} and from_unixtime(active_time,'yyyyMMdd')<={dt57})t25
       on t1.driver_id=t25.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt61} and from_unixtime(active_time,'yyyyMMdd')<={dt67})t26
       on t1.driver_id=t26.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt71} and from_unixtime(active_time,'yyyyMMdd')<={dt77})t27
       on t1.driver_id=t27.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt81} and from_unixtime(active_time,'yyyyMMdd')<={dt87})t28
       on t1.driver_id=t28.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt91} and from_unixtime(active_time,'yyyyMMdd')<={dt97})t214
       on t1.driver_id=t214.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt101} and from_unixtime(active_time,'yyyyMMdd')<={dt107})t230
       on t1.driver_id=t230.driver_id  
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt11} and dt<={dt17})t31
       on t1.driver_id=t31.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt21} and dt<={dt27})t32
       on t1.driver_id=t32.driver_id 
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt31} and dt<={dt37})t33
       on t1.driver_id=t33.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt41} and dt<={dt47})t34
       on t1.driver_id=t34.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt51} and dt<={dt57})t35
       on t1.driver_id=t35.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt61} and dt<={dt67})t36
       on t1.driver_id=t36.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt71} and dt<={dt77})t37
       on t1.driver_id=t37.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt81} and dt<={dt87})t38
       on t1.driver_id=t38.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt91} and dt<={dt97})t314
       on t1.driver_id=t314.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where dt>={dt101} and dt<={dt107})t330
       on t1.driver_id=t330.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt11} and dt<={dt17})t41
       on t1.driver_id=t41.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt21} and dt<={dt27})t42
       on t1.driver_id=t42.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt31} and dt<={dt37})t43
       on t1.driver_id=t43.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt41} and dt<={dt47})t44
       on t1.driver_id=t44.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt51} and dt<={dt57})t45
       on t1.driver_id=t45.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt61} and dt<={dt67})t46
       on t1.driver_id=t46.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt71} and dt<={dt77})t47
       on t1.driver_id=t47.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt81} and dt<={dt87})t48
       on t1.driver_id=t48.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt91} and dt<={dt97})t414
       on t1.driver_id=t414.driver_id
       left join (select distinct(driver_id) as driver_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day 
       where accept_status=1 and  dt>={dt101} and dt<={dt107})t430
       on t1.driver_id=t430.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt11} and dt<={dt17})t51
       on t1.driver_id=t51.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt21} and dt<={dt27})t52
       on t1.driver_id=t52.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt31} and dt<={dt37})t53
       on t1.driver_id=t53.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt41} and dt<={dt47})t54
       on t1.driver_id=t54.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt51} and dt<={dt57})t55
       on t1.driver_id=t55.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt61} and dt<={dt67})t56
       on t1.driver_id=t56.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt71} and dt<={dt77})t57
       on t1.driver_id=t57.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt81} and dt<={dt87})t58
       on t1.driver_id=t58.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt91} and dt<={dt97})t514
       on t1.driver_id=t514.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt101} and dt<={dt107})t530
       on t1.driver_id=t530.driver_id
       where t1.dt={dt_today} and 
       from_unixtime(create_time,'yyyyMMdd')>={dt11} and from_unixtime(create_time,'yyyyMMdd')<={dt17}
       """.format(dt_today=dt_today,dt11=dt_1_mon,dt17=dt_1_sta,dt21=dt_2_mon,dt27=dt_2_sta,dt31=dt_3_mon,dt37=dt_3_sta,
                  dt41=dt_4_mon,dt47=dt_4_sta,dt51=dt_5_mon,dt57=dt_5_sta,dt61=dt_6_mon,dt67=dt_6_sta,
                  dt71=dt_7_mon, dt77=dt_7_sta,dt81=dt_8_mon,dt87=dt_8_sta,dt91=dt_9_mon,dt97=dt_9_sta,
                  dt101=dt_10_mon, dt107=dt_10_sta)
       print sql
       ychive().query(sql)
       start_time+=604800

