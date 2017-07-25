#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-12"
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
       sql="""insert overwrite table yc_dm_mp.dm_user_act_series_detail_info_week partition(dt={dt11})
       select t1.user_id,
       t1.user_id,t32.user_id,t33.user_id,t34.user_id,t35.user_id,t36.user_id,t37.user_id,t38.user_id,t314.user_id,t330.user_id,
       t1.user_id,t42.user_id,t43.user_id,t44.user_id,t45.user_id,t46.user_id,t47.user_id,t48.user_id,t414.user_id,t430.user_id,
       t51.user_id,t52.user_id,t53.user_id,t54.user_id,t55.user_id,t56.user_id,t57.user_id,t58.user_id,t514.user_id,t530.user_id
       from yc_bit.user_jihuo_order_new t1
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt11} and dt<={dt17})t31
       on t1.user_id=t31.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt21} and dt<={dt27})t32
       on t1.user_id=t32.user_id 
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt31} and dt<={dt37})t33
       on t1.user_id=t33.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt41} and dt<={dt47})t34
       on t1.user_id=t34.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt51} and dt<={dt57})t35
       on t1.user_id=t35.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt61} and dt<={dt67})t36
       on t1.user_id=t36.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt71} and dt<={dt77})t37
       on t1.user_id=t37.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt81} and dt<={dt87})t38
       on t1.user_id=t38.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt91} and dt<={dt97})t314
       on t1.user_id=t314.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where dt>={dt101} and dt<={dt107})t330
       on t1.user_id=t330.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt11} and dt<={dt17})t41
       on t1.user_id=t41.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt21} and dt<={dt27})t42
       on t1.user_id=t42.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt31} and dt<={dt37})t43
       on t1.user_id=t43.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt41} and dt<={dt47})t44
       on t1.user_id=t44.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt51} and dt<={dt57})t45
       on t1.user_id=t45.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt61} and dt<={dt67})t46
       on t1.user_id=t46.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt71} and dt<={dt77})t47
       on t1.user_id=t47.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt81} and dt<={dt87})t48
       on t1.user_id=t48.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt91} and dt<={dt97})t414
       on t1.user_id=t414.user_id
       left join (select distinct(user_id) as user_id from yc_dw_mp.dw_service_order_userid_unique_day 
       where status=7 and  dt>={dt101} and dt<={dt107})t430
       on t1.user_id=t430.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt11} and dt<={dt17} and type=2)t51
       on t1.user_id=t51.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt21} and dt<={dt27} and type=2)t52
       on t1.user_id=t52.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt31} and dt<={dt37} and type=2)t53
       on t1.user_id=t53.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt41} and dt<={dt47} and type=2)t54
       on t1.user_id=t54.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt51} and dt<={dt57} and type=2)t55
       on t1.user_id=t55.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt61} and dt<={dt67} and type=2)t56
       on t1.user_id=t56.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt71} and dt<={dt77} and type=2)t57
       on t1.user_id=t57.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt81} and dt<={dt87} and type=2)t58
       on t1.user_id=t58.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt91} and dt<={dt97} and type=2)t514
       on t1.user_id=t514.user_id
       left join(select distinct(user_id) as user_id from yc_dw_mp.dw_user_charge_info_day 
       where dt>={dt101} and dt<={dt107} and type=2)t530
       on t1.user_id=t530.user_id
       where 
       from_unixtime(t1.active_time,'yyyyMMdd')>={dt11} and from_unixtime(t1.active_time,'yyyyMMdd')<={dt17}
       """.format(dt11=dt_1_mon,dt17=dt_1_sta,dt21=dt_2_mon,dt27=dt_2_sta,dt31=dt_3_mon,dt37=dt_3_sta,
                  dt41=dt_4_mon,dt47=dt_4_sta,dt51=dt_5_mon,dt57=dt_5_sta,dt61=dt_6_mon,dt67=dt_6_sta,
                  dt71=dt_7_mon, dt77=dt_7_sta,dt81=dt_8_mon,dt87=dt_8_sta,dt91=dt_9_mon,dt97=dt_9_sta,
                  dt101=dt_10_mon, dt107=dt_10_sta)
       print sql
       ychive().query(sql)
       start_time+=604800

