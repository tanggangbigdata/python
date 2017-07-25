#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-03-21"

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
       sql1="""
       insert overwrite table yc_dm_mp.dm_user_collect_info partition(dt={dt})
       select t1.user_id,t2.create_time as register_time,t3.create_time as bound_card_time,
t4.first_charge_time,T8.min_time as first_create_order_time,t5.active_time,t1.city,t6.points,t6.level_id,
t5.service_order_id,T8.max8_time,T7.service_order_id,T9.max9_time as last_charge_time,T8.create_order_num,T8.finish_order_num,
T8.order_sum_amount,T9.charge_num,T9.charge_amount,t10.amount
from yc_dw_mp.dw_user_base_info t1
left join (select * from yc_bit.account_balance where dt={dt}) t10
on t1.account_id=t10.account_id
left join (select * from yc_dw_mp.dw_user_reg_info where dt={dt})t2 on t1.user_id=t2.user_id
left join (select * from yc_dm_mp.dm_user_first_charge where dt={dt})t4 on t1.user_id=t4.user_id
left join yc_bit.user_jihuo_order_new t5 on t1.user_id=t5.user_id
left join (select *,row_number() over(partition by user_id order by update_time desc)num6 from yc_ods.fu_user_points where dt<={dt})t6
on t1.user_id=t6.user_id and t6.num6=1
left join(select *,row_number() over(partition by account_id order by create_time desc)num3 from yc_ods.fa_bound where dt<={dt})t3
on t10.account_id=t3.account_id and t3.num3=1
left join(
select user_id,service_order_id from
(select user_id,service_order_id,update_time,row_number() over(partition by user_id order by update_time desc)num7 from yc_dm_mp.dm_service_order_all_unique
where dt={dt})t7
where t7.num7=1)T7 on t1.user_id=T7.user_id
left join (
select user_id,min(create_time) as min_time,max(create_time) as max8_time,count(distinct service_order_id) as create_order_num,
count(distinct(if(status=7,service_order_id,null))) as finish_order_num,
sum(if(status=7,origin_amount,0)) as order_sum_amount from yc_dm_mp.dm_service_order_all_unique where dt={dt} group by user_id)T8 on t1.user_id=T8.user_id
left join (
select account_id,max(create_time) as max9_time,count(balance_record_id) as charge_num,sum(amount) as charge_amount from
(select *,row_number() over(partition by balance_record_id,account_id order by create_time desc)num9 from yc_ods.fa_balance_record
where dt<={dt} and reason=1 and type=2)t9
where t9.num9=1 group by account_id)T9 on t10.account_id=T9.account_id
where t1.dt={dt}
       """.format(dt=dt)
       ychive().query(sql1)
#       sql2="""
#
 #      """.format(dt=dt,dt_1day_ago=dt_1day_ago)
 #      print sql2
#       ychive().query(sql2)
       start_time+=86400