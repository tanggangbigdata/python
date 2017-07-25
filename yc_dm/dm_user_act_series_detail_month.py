#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-12"
#司机注册后的后续月行为


from core.ychive import ychive
from core.dateTime import dateTime
from core.connmysql import connmysql
from dm_driver_reg_series_detail_month import dt_9list
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
       sql="""insert overwrite table yc_dm_mp.dm_user_act_series_detail_info_mon partition(dt={dt_mon1})
       select t1.user_id,
       t1.user_id,t32.user_id,t33.user_id,t34.user_id,t35.user_id,t36.user_id,t37.user_id,t38.user_id,
       t1.user_id,t42.user_id,t43.user_id,t44.user_id,t45.user_id,t46.user_id,t47.user_id,t48.user_id,
       t51.user_id,t52.user_id,t53.user_id,t54.user_id,t55.user_id,t56.user_id,t57.user_id,t58.user_id
       from yc_bit.user_jihuo_order_new t1
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt_mon1} and dt<{dt_mon2})t31
       on t1.user_id=t31.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt_mon2} and dt<{dt_mon3})t32
       on t1.user_id=t32.user_id 
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt_mon3} and dt<{dt_mon4})t33
       on t1.user_id=t33.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt_mon4} and dt<{dt_mon5})t34
       on t1.user_id=t34.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt_mon5} and dt<{dt_mon6})t35
       on t1.user_id=t35.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt_mon6} and dt<{dt_mon7})t36
       on t1.user_id=t36.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt_mon7} and dt<{dt_mon8})t37
       on t1.user_id=t37.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt_mon8} and dt<{dt_mon9})t38
       on t1.user_id=t38.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt_mon1} and dt<{dt_mon2})t41
       on t1.user_id=t41.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt_mon2} and dt<{dt_mon3})t42
       on t1.user_id=t42.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt_mon3} and dt<{dt_mon4})t43
       on t1.user_id=t43.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt_mon4} and dt<{dt_mon5})t44
       on t1.user_id=t44.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt_mon5} and dt<{dt_mon6})t45
       on t1.user_id=t45.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt_mon6} and dt<{dt_mon7})t46
       on t1.user_id=t46.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt_mon7} and dt<{dt_mon8})t47
       on t1.user_id=t47.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt_mon8} and dt<{dt_mon9})t48
       on t1.user_id=t48.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt_mon1} and dt<{dt_mon2} and type=2)t51
       on t1.user_id=t51.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt_mon2} and dt<{dt_mon3} and type=2)t52
       on t1.user_id=t52.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt_mon3} and dt<{dt_mon4} and type=2)t53
       on t1.user_id=t53.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt_mon4} and dt<{dt_mon5} and type=2)t54
       on t1.user_id=t54.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt_mon5} and dt<{dt_mon6} and type=2)t55
       on t1.user_id=t55.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt_mon6} and dt<{dt_mon7} and type=2)t56
       on t1.user_id=t56.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt_mon7} and dt<{dt_mon8} and type=2)t57
       on t1.user_id=t57.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt_mon8} and dt<{dt_mon9} and type=2)t58
       on t1.user_id=t58.user_id
       where 
       from_unixtime(t1.active_time,'yyyyMMdd')>={dt_mon1} and from_unixtime(t1.active_time,'yyyyMMdd')<{dt_mon2}
       """.format(dt_mon1=dt_mon1,dt_mon2=dt_mon2,dt_mon3=dt_mon3,dt_mon4=dt_mon4,dt_mon5=dt_mon5,dt_mon6=dt_mon6,
                  dt_mon7=dt_mon7,dt_mon8=dt_mon8,dt_mon9=dt_mon9)
       print sql
       ychive().query(sql)
       start_time += 86400







