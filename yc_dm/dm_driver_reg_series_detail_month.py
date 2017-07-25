#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-06"
#司机注册后的后续月行为


from core.ychive import ychive
from core.dateTime import dateTime
from core.connmysql import connmysql
from core.queue import Td
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
       dt_today = time.strftime('%Y%m%d', time.localtime(int(time.time()) - 86400))
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
       sql="""insert overwrite table yc_dm_mp.dm_driver_reg_series_detail_info_mon partition(dt={dt_mon1})
       select t1.driver_id,
       t21.driver_id,t22.driver_id,t23.driver_id,t24.driver_id,t25.driver_id,t26.driver_id,t27.driver_id,t28.driver_id,
       t31.driver_id,t32.driver_id,t33.driver_id,t34.driver_id,t35.driver_id,t36.driver_id,t37.driver_id,t38.driver_id,
       t41.driver_id,t42.driver_id,t43.driver_id,t44.driver_id,t45.driver_id,t46.driver_id,t47.driver_id,t48.driver_id,
       t51.driver_id,t52.driver_id,t53.driver_id,t54.driver_id,t55.driver_id,t56.driver_id,t57.driver_id,t58.driver_id
       from yc_dw_mp.dw_driver_reg_info t1
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt_mon1} and from_unixtime(active_time,'yyyyMMdd')<{dt_mon2})t21
       on t1.driver_id=t21.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt_mon2} and from_unixtime(active_time,'yyyyMMdd')<{dt_mon3})t22
       on t1.driver_id=t22.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt_mon3} and from_unixtime(active_time,'yyyyMMdd')<{dt_mon4})t23
       on t1.driver_id=t23.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt_mon4} and from_unixtime(active_time,'yyyyMMdd')<{dt_mon5})t24
       on t1.driver_id=t24.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt_mon5} and from_unixtime(active_time,'yyyyMMdd')<{dt_mon6})t25
       on t1.driver_id=t25.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt_mon6} and from_unixtime(active_time,'yyyyMMdd')<{dt_mon7})t26
       on t1.driver_id=t26.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt_mon7} and from_unixtime(active_time,'yyyyMMdd')<{dt_mon8})t27
       on t1.driver_id=t27.driver_id
       left join (select driver_id from yc_dm_mp.dm_driver_jihuo_all 
       where from_unixtime(active_time,'yyyyMMdd')>={dt_mon8} and from_unixtime(active_time,'yyyyMMdd')<9{dt_mon9})t28
       on t1.driver_id=t28.driver_id
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
       where status=7 and dt>={dt_mon1} and dt<{dt_mon2})t51
       on t1.driver_id=t51.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt_mon2} and dt<{dt_mon3})t52
       on t1.driver_id=t52.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt_mon3} and dt<{dt_mon4})t53
       on t1.driver_id=t53.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt_mon4} and dt<{dt_mon5})t54
       on t1.driver_id=t54.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt_mon5} and dt<{dt_mon6})t55
       on t1.driver_id=t55.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt_mon6} and dt<{dt_mon7})t56
       on t1.driver_id=t56.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt_mon7} and dt<{dt_mon8})t57
       on t1.driver_id=t57.driver_id
       left join(select distinct(driver_id) as driver_id from yc_dw_mp.dw_service_order_driverid_unique_day 
       where status=7 and dt>={dt_mon8} and dt<{dt_mon9})t58
       on t1.driver_id=t58.driver_id
       where t1.dt={dt_today} and 
       from_unixtime(create_time,'yyyyMMdd')>={dt_mon1} and from_unixtime(create_time,'yyyyMMdd')<{dt_mon2}
       """.format(dt_today=dt_today,dt_mon1=dt_mon1,dt_mon2=dt_mon2,dt_mon3=dt_mon3,dt_mon4=dt_mon4,dt_mon5=dt_mon5,dt_mon6=dt_mon6,
                  dt_mon7=dt_mon7,dt_mon8=dt_mon8,dt_mon9=dt_mon9)
       print sql
       ychive().query(sql)
       start_time += 86400

    # dt_cur_mon_day = '20170701'
    # dt_list = []
    # first_time = start_time-86400
    # dt = time.strftime('%Y%m%d', time.localtime(first_time))
    # dt_list.append(dt)
    # for i in range(1, 8):
    #      dt_cur_mon_day=time.strftime('%Y%m%d', time.localtime(first_time-86400))
    #      monthRange = calendar.monthrange(int(dt_cur_mon_day[0:4]), int(dt_cur_mon_day[4:6]))
    #      monday = monthRange[1]
    #      print monday
    #      timerange = int(int(monday) * 86400)
    #      print timerange
    #      first_time = int(first_time) - timerange
    #      dt = time.strftime('%Y%m%d', time.localtime(first_time))
    #      dt_cur_mon_day = dt
    #      dt_list.append(dt)
    #    # else:
    #    #    dt_cur_mon_day = time.strftime('%Y%m%d', time.localtime(first_time - 86400))
    #    #    monthRange = calendar.monthrange(int(dt_cur_mon_day[0:4]), int(dt_cur_mon_day[4:6]))
    #    #    monday = monthRange[1]
    #    #    print monday
    #    #    timerange = int(int(monday) * 86400)
    #    #    print timerange
    #    #    first_time = int(first_time) - timerange
    #    #    dt = time.strftime('%Y%m%d', time.localtime(first_time))
    #    #    print dt
    #    #    dt_cur_mon_day = dt
    #    #    dt_list.append(dt)
    # print dt_list



 # def month(self,argv):
 #     dt,dty,start_time,end_time = dateTime().getDay(argv)
 #     cur_mon_time = start_time
 #     mon1_ago_time = start_time - (mon_1_range + 1) * 86400
 #     mon2_ago_time = start_time - (mon_2_range + 1) * 86400
 #     mon3_ago_time = start_time - (mon_3_range + 1) * 86400
 #     mon4_ago_time = start_time - (mon_4_range + 1) * 86400
 #     mon5_ago_time = start_time - (mon_5_range + 1) * 86400
 #     mon6_ago_time = start_time - (mon_6_range + 1) * 86400
 #     mon7_ago_time = start_time - (mon_7_range + 1) * 86400
 #
 #     dt_cur_mon_day = time.strftime('%Y%m%d', time.localtime(cur_mon_time))
 #     dt_1mon_ago_day = time.strftime('%Y%m%d', time.localtime(mon1_ago_time))
 #     dt_2mon_ago_day = time.strftime('%Y%m%d', time.localtime(mon2_ago_time))
 #     dt_3mon_ago_day = time.strftime('%Y%m%d', time.localtime(mon3_ago_time))
 #     dt_4mon_ago_day = time.strftime('%Y%m%d', time.localtime(mon4_ago_time))
 #     dt_5mon_ago_day = time.strftime('%Y%m%d', time.localtime(mon5_ago_time))
 #     dt_6mon_ago_day = time.strftime('%Y%m%d', time.localtime(mon6_ago_time))
 #     dt_7mon_ago_day = time.strftime('%Y%m%d', time.localtime(mon7_ago_time))

def dt_9list(start_time):
    dt_list = []
    first_time = start_time
    dt = time.strftime('%Y%m%d', time.localtime(first_time-86400))
    dt_cur_mon_day = dt
    dt_list.append(dt)
    for i in range(1, 9):
        dt_cur_mon_day = time.strftime('%Y%m%d', time.localtime(first_time - 86400))
        monthRange = calendar.monthrange(int(dt_cur_mon_day[0:4]), int(dt_cur_mon_day[4:6]))
        monday = monthRange[1]
        print monday
        timerange = int(int(monday) * 86400)
        print timerange
        first_time = int(first_time) + timerange
        dt = time.strftime('%Y%m%d', time.localtime(first_time-86400))
        dt_cur_mon_day = dt
        dt_list.append(dt)
    return dt_list




