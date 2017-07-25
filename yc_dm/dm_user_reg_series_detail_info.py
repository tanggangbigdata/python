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

       sql1="""insert overwrite table test.dm_driver_reg_series_detail_info_day_tmp1
       select t1.user_id,
       t21.user_id,t22.user_id,t23.user_id,t24.user_id,t25.user_id,t26.user_id,t27.user_id,t28.user_id,t214.user_id,t230.user_id
       from yc_dw_mp.dw_driver_reg_info t1
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt})t21
       on t1.user_id=t21.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt2})t22
       on t1.user_id=t22.user_id 
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt3})t23
       on t1.user_id=t23.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt4})t24
       on t1.user_id=t24.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt5})t25
       on t1.user_id=t25.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt6})t26
       on t1.user_id=t26.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt7})t27
       on t1.user_id=t27.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt8})t28
       on t1.user_id=t28.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt14})t214
       on t1.user_id=t214.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where dt={dt30})t230
       on t1.user_id=t230.user_id
       where t1.dt=20170601 and from_unixtime(create_time,'yyyyMMdd')={dt}
       """.format(dt=dt,dt2=dt2,dt3=dt3,dt4=dt4,dt5=dt5,dt6=dt6,dt7=dt7,dt8=dt8,dt14=dt14,dt30=dt30)
#       ychive().query(sql1)

       sql2="""insert overwrite table yc_tmp.dm_driver_reg_series_detail_info_day_tmp2
       select t1.user_id,
       t31.user_id,t32.user_id,t33.user_id,t34.user_id,t35.user_id,t36.user_id,t37.user_id,t38.user_id,t314.user_id,t330.user_id
       from yc_dw_mp.dw_driver_reg_info t1
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt})t31
       on t1.user_id=t31.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt2})t32
       on t1.user_id=t32.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt3})t33
       on t1.user_id=t33.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt4})t34
       on t1.user_id=t34.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt5})t35
       on t1.user_id=t35.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt6})t36
       on t1.user_id=t36.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt7})t37
       on t1.user_id=t37.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt8})t38
       on t1.user_id=t38.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt14})t314
       on t1.user_id=t314.user_id
       left join (select user_id from yc_dw_mp.dw_dispatch_detail_driverid_unique_day where accept_status=1 and  dt={dt30})t330
       on t1.user_id=t330.user_id
       where t1.dt=20170601 and from_unixtime(create_time,'yyyyMMdd')={dt}
       """.format(dt=dt,dt2=dt2,dt3=dt3,dt4=dt4,dt5=dt5,dt6=dt6,dt7=dt7,dt8=dt8,dt14=dt14,dt30=dt30)
#       ychive().query(sql2)

       sql3="""insert overwrite table yc_tmp.dw_service_order_driverid_unique_day_tmp
       select t1.user_id,
       t41.user_id,t42.user_id,t43.user_id,t44.user_id,t45.user_id,t46.user_id,t47.user_id,t48.user_id,t414.user_id,t430.user_id
       from yc_dw_mp.dw_driver_reg_info t1
       left join(select user_id from yc_dw_mp.dw_service_order_driverid_unique_day where status=7 and dt={dt})t41
       on t1.user_id=t41.user_id
       left join(select user_id from yc_dw_mp.dw_service_order_driverid_unique_day where status=7 and dt={dt2})t42
       on t1.user_id=t42.user_id
       left join(select user_id from yc_dw_mp.dw_service_order_driverid_unique_day where status=7 and dt={dt3})t43
       on t1.user_id=t43.user_id
       left join(select user_id from yc_dw_mp.dw_service_order_driverid_unique_day where status=7 and dt={dt4})t44
       on t1.user_id=t44.user_id
       left join(select user_id from yc_dw_mp.dw_service_order_driverid_unique_day where status=7 and dt={dt5})t45
       on t1.user_id=t45.user_id
       left join(select user_id from yc_dw_mp.dw_service_order_driverid_unique_day where status=7 and dt={dt6})t46
       on t1.user_id=t46.user_id
       left join(select user_id from yc_dw_mp.dw_service_order_driverid_unique_day where status=7 and dt={dt7})t47
       on t1.user_id=t47.user_id
       left join(select user_id from yc_dw_mp.dw_service_order_driverid_unique_day where status=7 and dt={dt8})t48
       on t1.user_id=t48.user_id
       left join(select user_id from yc_dw_mp.dw_service_order_driverid_unique_day where status=7 and dt={dt14})t414
       on t1.user_id=t414.user_id
       left join(select user_id from yc_dw_mp.dw_service_order_driverid_unique_day where status=7 and dt={dt30})t430
       on t1.user_id=t430.user_id
       where t1.dt=20170601 and from_unixtime(create_time,'yyyyMMdd')={dt}
       """.format(dt=dt,dt2=dt2,dt3=dt3,dt4=dt4,dt5=dt5,dt6=dt6,dt7=dt7,dt8=dt8,dt14=dt14,dt30=dt30)
#       ychive().query(sql3)

       sql="""insert overwrite table yc_dm_mp.dm_user_reg_series_detail_info_day partition(dt={dt})
       select t1.user_id,
       t21.user_id,t22.user_id,t23.user_id,t24.user_id,t25.user_id,t26.user_id,t27.user_id,t28.user_id,t214.user_id,t230.user_id,
       t31.user_id,t32.user_id,t33.user_id,t34.user_id,t35.user_id,t36.user_id,t37.user_id,t38.user_id,t314.user_id,t330.user_id,
       t41.user_id,t42.user_id,t43.user_id,t44.user_id,t45.user_id,t46.user_id,t47.user_id,t48.user_id,t414.user_id,t430.user_id,
       t51.user_id,t52.user_id,t53.user_id,t54.user_id,t55.user_id,t56.user_id,t57.user_id,t58.user_id,t514.user_id,t530.user_id
       from yc_dw_mp.dw_user_reg_info t1
       left join (select * from yc_bit.user_jihuo_order_new where from_unixtime(active_time,'yyyyMMdd')={dt})t21
       on t1.user_id=t21.user_id
       left join (select * from yc_bit.user_jihuo_order_new where from_unixtime(active_time,'yyyyMMdd')={dt2})t22
       on t1.user_id=t22.user_id
       left join (select * from yc_bit.user_jihuo_order_new where from_unixtime(active_time,'yyyyMMdd')={dt3})t23
       on t1.user_id=t23.user_id
       left join (select * from yc_bit.user_jihuo_order_new where from_unixtime(active_time,'yyyyMMdd')={dt4})t24
       on t1.user_id=t24.user_id
       left join (select * from yc_bit.user_jihuo_order_new where from_unixtime(active_time,'yyyyMMdd')={dt5})t25
       on t1.user_id=t25.user_id
       left join (select * from yc_bit.user_jihuo_order_new where from_unixtime(active_time,'yyyyMMdd')={dt6})t26
       on t1.user_id=t26.user_id
       left join (select * from yc_bit.user_jihuo_order_new where from_unixtime(active_time,'yyyyMMdd')={dt7})t27
       on t1.user_id=t27.user_id
       left join (select * from yc_bit.user_jihuo_order_new where from_unixtime(active_time,'yyyyMMdd')={dt8})t28
       on t1.user_id=t28.user_id
       left join (select * from yc_bit.user_jihuo_order_new where from_unixtime(active_time,'yyyyMMdd')={dt14})t214
       on t1.user_id=t214.user_id
       left join (select * from yc_bit.user_jihuo_order_new where from_unixtime(active_time,'yyyyMMdd')={dt30})t230
       on t1.user_id=t230.user_id
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
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  type=2 and  dt={dt})t51
       on t1.user_id=t51.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  type=2 and dt={dt2})t52
       on t1.user_id=t52.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  type=2 and dt={dt3})t53
       on t1.user_id=t53.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  type=2 and dt={dt4})t54
       on t1.user_id=t54.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  type=2 and dt={dt5})t55
       on t1.user_id=t55.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  type=2 and dt={dt6})t56
       on t1.user_id=t56.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  type=2 and dt={dt7})t57
       on t1.user_id=t57.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  type=2 and dt={dt8})t58
       on t1.user_id=t58.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  type=2 and dt={dt14})t514
       on t1.user_id=t514.user_id
       left join(select user_id from yc_dw_mp.dw_user_charge_info_day where  type=2 and dt={dt30})t530
       on t1.user_id=t530.user_id
       where t1.dt={dt_today} and from_unixtime(create_time,'yyyyMMdd')={dt}
       """.format(dt_today=dt_today,dt=dt,dt2=dt2,dt3=dt3,dt4=dt4,dt5=dt5,dt6=dt6,dt7=dt7,dt8=dt8,dt14=dt14,dt30=dt30)
       # sql = """insert overwrite table yc_dm_mp.dm_driver_reg_series_detail_info_day partition(dt={dt})
       #        select t1.user_id,
       #        t21.user_id,t22.user_id,t23.user_id,t24.user_id,t25.user_id,t26.user_id,t27.user_id,t28.user_id,t214.user_id,t230.user_id,
       #        t3.user_id_dispatch_1day,t3.user_id_dispatch_2day,t3.user_id_dispatch_3day,t3.user_id_dispatch_4day,t3.user_id_dispatch_5day,
       #        t3.user_id_dispatch_6day,t3.user_id_dispatch_7day,t3.user_id_dispatch_8day,t3.user_id_dispatch_14day,t3.user_id_dispatch_30day,
       #        t4.user_id_dispatch_accept_1day,t4.user_id_dispatch_accept_2day,t4.user_id_dispatch_accept_3day,t4.user_id_dispatch_accept_4day,t4.user_id_dispatch_accept_5day,
       #        t4.user_id_dispatch_accept_6day,t4.user_id_dispatch_accept_7day,t4.user_id_dispatch_accept_8day,t4.user_id_dispatch_accept_14day,t4.user_id_dispatch_accept_30day,
       #        t5.user_id_order_finish_1day,t5.user_id_order_finish_2day,t5.user_id_order_finish_3day,t5.user_id_order_finish_4day,t5.user_id_order_finish_5day,
       #        t5.user_id_order_finish_6day,t5.user_id_order_finish_7day,t5.user_id_order_finish_8day,t5.user_id_order_finish_14day,t5.user_id_order_finish_30day
       #        from yc_dw_mp.dw_driver_reg_info t1
       #        left join (select * from yc_dm_mp.dm_driver_jihuo_all where from_unixtime(active_time,'yyyyMMdd')={dt})t21
       #        on t1.user_id=t21.user_id
       #        left join (select * from yc_dm_mp.dm_driver_jihuo_all where from_unixtime(active_time,'yyyyMMdd')={dt2})t22
       #        on t1.user_id=t22.user_id
       #        left join (select * from yc_dm_mp.dm_driver_jihuo_all where from_unixtime(active_time,'yyyyMMdd')={dt3})t23
       #        on t1.user_id=t23.user_id
       #        left join (select * from yc_dm_mp.dm_driver_jihuo_all where from_unixtime(active_time,'yyyyMMdd')={dt4})t24
       #        on t1.user_id=t24.user_id
       #        left join (select * from yc_dm_mp.dm_driver_jihuo_all where from_unixtime(active_time,'yyyyMMdd')={dt5})t25
       #        on t1.user_id=t25.user_id
       #        left join (select * from yc_dm_mp.dm_driver_jihuo_all where from_unixtime(active_time,'yyyyMMdd')={dt6})t26
       #        on t1.user_id=t26.user_id
       #        left join (select * from yc_dm_mp.dm_driver_jihuo_all where from_unixtime(active_time,'yyyyMMdd')={dt7})t27
       #        on t1.user_id=t27.user_id
       #        left join (select * from yc_dm_mp.dm_driver_jihuo_all where from_unixtime(active_time,'yyyyMMdd')={dt8})t28
       #        on t1.user_id=t28.user_id
       #        left join (select * from yc_dm_mp.dm_driver_jihuo_all where from_unixtime(active_time,'yyyyMMdd')={dt14})t214
       #        on t1.user_id=t214.user_id
       #        left join (select * from yc_dm_mp.dm_driver_jihuo_all where from_unixtime(active_time,'yyyyMMdd')={dt30})t230
       #        on t1.user_id=t230.user_id
       #        left join test.dm_driver_reg_series_detail_info_day_tmp1 t3
       #        on t1.user_id=t3.user_id_reg
       #        left join test.dm_driver_reg_series_detail_info_day_tmp2 t4
       #        on t1.user_id=t4.user_id_reg
       #        left join test.dw_service_order_driverid_unique_day_tmp t5
       #        on t1.user_id=t5.user_id_reg
       #        where t1.dt=20170601 and from_unixtime(create_time,'yyyyMMdd')={dt}
       #        """.format(dt=dt, dt2=dt2, dt3=dt3, dt4=dt4, dt5=dt5, dt6=dt6, dt7=dt7, dt8=dt8, dt14=dt14, dt30=dt30)
       ychive().query(sql)
       start_time+=86400

