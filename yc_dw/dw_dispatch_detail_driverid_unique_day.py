#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-01"


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
#       dt_1day_ago=time.strftime('%Y%m%d',time.localtime(start_time_1day_ago))
       sql="""insert overwrite table yc_dw_mp.dw_dispatch_detail_driverid_unique_day partition(dt={dt})
       select 
       datetime,
       service_order_id,
       round,
       batch,
       flag,
       driver_id,
       distance,
       dispatch_time,
       dispatch_lat,
       dispatch_lng,
       dispatch_total_rate,
       dispatch_snapshot,
       response_time,
       accept_status,
       response_lat,
       response_lng,
       response_distance,
       response_time_length,
       decision_time,
       decision_total_rate,
       decision_result,
       decision_failure_reason,
       decision_msg_snapshot,
       subtract_amount,
       add_price_set,
       response_snapshot,
       is_assigned,
       route_distance,
       route_time_length,
       distance_time_length,
       driver_bidding_rate,
       driver_estimate_price,
       city
       from
       (select *,row_number() over(partition by driver_id,accept_status order by dispatch_time)num from yc_dw_mp.dispatch_detail_info
       where dt={dt})t1
       where t1.num=1 
       """.format(dt=dt)
       ychive().query(sql)
       start_time+=86400

