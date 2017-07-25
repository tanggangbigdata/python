#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-05-31"

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
       dt = time.strftime('%Y%m%d',time.localtime(start_time))
       dt_1day_ago=time.strftime('%Y%m%d',time.localtime(start_time_1day_ago))
       sql="""insert overwrite table yc_dm_mp.dm_driver_jihuo_all
       select t1.driver_id,t1.user_id,t1.service_order_id,t1.car_type_id,t1.product_type_id,t1.end_time
       from(select driver_id,user_id,service_order_id,car_type_id,product_type_id,end_time,update_time,status,
       row_number() over(partition by driver_id order by end_time)num from yc_dm_mp.dm_service_order_all_unique
       where dt={dt} and status=7)t1
       where t1.num=1
       """.format(dt=dt)
       ychive().query(sql)
       start_time+=86400
