#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-12"
#司机注册后的后续行为
#为了使数据保持一致，t1.user  select了两次


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

       sql="""insert overwrite table yc_dm_mp.dm_user_act_series_detail_info_day partition(dt={dt})
       select t1.user_id,
       t1.user_id,t32.user_id,t33.user_id,t34.user_id,t35.user_id,t36.user_id,t37.user_id,t38.user_id,t314.user_id,t330.user_id,
       t1.user_id,t42.user_id,t43.user_id,t44.user_id,t45.user_id,t46.user_id,t47.user_id,t48.user_id,t414.user_id,t430.user_id,
       t51.user_id,t52.user_id,t53.user_id,t54.user_id,t55.user_id,t56.user_id,t57.user_id,t58.user_id,t514.user_id,t530.user_id
       from yc_bit.user_jihuo_order_new t1
       left join (select distinct(user_id) from yc_dw_mp.dw_service_order_userid_unique_day where dt={dt})t31
       on t1.user_id=t31.user_id
       left join (select distinct(user_id) from yc_dw_mp.dw_service_order_userid_unique_day where dt={dt2})t32
       on t1.user_id=t32.user_id
       left join (select distinct(user_id) from yc_dw_mp.dw_service_order_userid_unique_day where dt={dt3})t33
       on t1.user_id=t33.user_id
       left join (select distinct(user_id) from yc_dw_mp.dw_service_order_userid_unique_day where dt={dt4})t34
       on t1.user_id=t34.user_id
       left join (select distinct(user_id) from yc_dw_mp.dw_service_order_userid_unique_day where dt={dt5})t35
       on t1.user_id=t35.user_id
       left join (select distinct(user_id) from yc_dw_mp.dw_service_order_userid_unique_day where dt={dt6})t36
       on t1.user_id=t36.user_id
       left join (select distinct(user_id) from yc_dw_mp.dw_service_order_userid_unique_day where dt={dt7})t37
       on t1.user_id=t37.user_id
       left join (select distinct(user_id) from yc_dw_mp.dw_service_order_userid_unique_day where dt={dt8})t38
       on t1.user_id=t38.user_id
       left join (select distinct(user_id) from yc_dw_mp.dw_service_order_userid_unique_day where dt={dt14})t314
       on t1.user_id=t314.user_id
       left join (select distinct(user_id) from yc_dw_mp.dw_service_order_userid_unique_day where dt={dt30})t330
       on t1.user_id=t330.user_id
       left join (select user_id from yc_dw_mp.dw_service_order_userid_unique_day where status=7 and  dt={dt})t41
       on t1.user_id=t41.user_id
       left join (select user_id from yc_dw_mp.dw_service_order_userid_unique_day where status=7 and  dt={dt2})t42
       on t1.user_id=t42.user_id
       left join (select user_id from yc_dw_mp.dw_service_order_userid_unique_day where status=7 and  dt={dt3})t43
       on t1.user_id=t43.user_id
       left join (select user_id from yc_dw_mp.dw_service_order_userid_unique_day where status=7 and  dt={dt4})t44
       on t1.user_id=t44.user_id
       left join (select user_id from yc_dw_mp.dw_service_order_userid_unique_day where status=7 and  dt={dt5})t45
       on t1.user_id=t45.user_id
       left join (select user_id from yc_dw_mp.dw_service_order_userid_unique_day where status=7 and  dt={dt6})t46
       on t1.user_id=t46.user_id
       left join (select user_id from yc_dw_mp.dw_service_order_userid_unique_day where status=7 and  dt={dt7})t47
       on t1.user_id=t47.user_id
       left join (select user_id from yc_dw_mp.dw_service_order_userid_unique_day where status=7 and  dt={dt8})t48
       on t1.user_id=t48.user_id
       left join (select user_id from yc_dw_mp.dw_service_order_userid_unique_day where status=7 and  dt={dt14})t414
       on t1.user_id=t414.user_id
       left join (select user_id from yc_dw_mp.dw_service_order_userid_unique_day where status=7 and  dt={dt30})t430
       on t1.user_id=t430.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  dt={dt} and type=2)t51
       on t1.user_id=t51.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  dt={dt2} and type=2)t52
       on t1.user_id=t52.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  dt={dt3} and type=2)t53
       on t1.user_id=t53.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  dt={dt4} and type=2)t54
       on t1.user_id=t54.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  dt={dt5} and type=2)t55
       on t1.user_id=t55.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  dt={dt6} and type=2)t56
       on t1.user_id=t56.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  dt={dt7} and type=2)t57
       on t1.user_id=t57.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  dt={dt8} and type=2)t58
       on t1.user_id=t58.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  dt={dt14} and type=2)t514
       on t1.user_id=t514.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  dt={dt30} and type=2)t530
       on t1.user_id=t530.user_id
       where from_unixtime(t1.active_time,'yyyyMMdd')={dt}
       """.format(dt=dt,dt2=dt2,dt3=dt3,dt4=dt4,dt5=dt5,dt6=dt6,dt7=dt7,dt8=dt8,dt14=dt14,dt30=dt30)
       ychive().query(sql)
       start_time+=86400

