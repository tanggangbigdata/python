#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-04-18"

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
       sql="""
        insert overwrite table yc_dm_mp.dm_dispatch_service_order_collect partition(dt={dt})
select COALESCE(t7.service_order_id,t1.service_order_id),
COALESCE(t7.dispatch_time,t1.dispatch_time),
COALESCE(t7.route_time_length,t1.route_time_length),
COALESCE(t7.response_time,t1.response_time),
COALESCE(t7.decision_time,t1.decision_time),
t2.driver_id,t2.user_id,t2.city,
get_json_object(t3.estimate_snap,'$.distance'),get_json_object(t3.estimate_snap,'$.time_length'),
t4.driver_id,
t5.dispatch_type,t5.decision_type,t5.driver_bidding_rate,t5.driver_estimate_price,
t6.driver_dispatch_num,t6.round_num,t6.batch_num,t6.driver_response_num,t6.driver_accept_num
from
yc_ods.ods_service_order t2
left join
(select *,row_number() over(partition by service_order_id order by dispatch_time desc)num1 from yc_dw_mp.dispatch_detail_info where dt={dt}) t1
 on t1.service_order_id=t2.service_order_id and t1.num1=1
left join
(select *,row_number() over(partition by service_order_id order by dispatch_time desc)num7 from yc_dw_mp.dispatch_detail_info where dt={dt} and decision_result=2) t7
on t1.service_order_id=t7.service_order_id and t7.num7=1
left join
(select*,row_number() over(partition by service_order_id order by update_time desc)num3 from yc_ods.ods_service_order_ext where dt={dt})t3
on t2.service_order_id=t3.service_order_id  and t3.num3=1
left join
(select *,row_number() over(partition by service_order_id order by dispatch_time desc)num4 from yc_dw_mp.dispatch_detail_info where dt={dt} and is_assigned=1) t4
on t1.service_order_id=t4.service_order_id and t4.num4=1
left join
(select *,row_number() over(partition by service_order_id order by dispatch_time desc)num5 from yc_dw_mp.dispatch_info where dt={dt}) t5
on t2.service_order_id=t5.service_order_id and t5.num5=1
left join
(select service_order_id,count(distinct driver_id) as driver_dispatch_num,
count(distinct round) as round_num ,
count(distinct round,batch) as batch_num,
count(if(accept_status=1,driver_id,null)) as driver_response_num,
count(if(decision_result!=0,driver_id,null)) as driver_accept_num
from yc_dw_mp.dispatch_detail_info where dt={dt} group by service_order_id) t6
on t1.service_order_id=t6.service_order_id
where t1.dt={dt} and t2.dt={dt} and t3.dt={dt} and (t1.service_order_id is not null or t7.service_order_id is not null)
       """.format(dt=dt)
       ychive().query(sql)
       start_time+=86400
