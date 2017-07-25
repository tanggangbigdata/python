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
        insert overwrite table yc_dm_mp.dm_user_first_charge partition(dt=20170507)
select t1.account_id as account_id,t1.activity_id,t3.user_id as user_id,t1.amount as first_charge_amount,t1.create_time as first_charge_time,
t2.amount as first_back_amount,t2.create_time as first_back_time
from
(select *,row_number() over(partition by account_id order by create_time)num1 from yc_bit.account_history where reason=1 and type in(2,4) and dt<=20170507)t1
left join
(select *,row_number() over(partition by account_id order by create_time)num2 from yc_bit.account_history where reason=16 and type in(2,4) and dt<=20170507)t2
on t1.account_id=t2.account_id and t2.num2=1
left join
(select user_id,account_id,row_number() over(partition by user_id order by create_time desc)num3 from yc_ods.fu_user where dt<=20170507)t3
on t1.account_id=t3.account_id and t3.num3=1
where t1.num1=1
       """
#       ychive().query(sql1)
       sql2="""
insert overwrite table yc_dm_mp.dm_user_first_charge partition(dt={dt})
       select COALESCE(T1.account_id,T2.account_id),
       COALESCE(T1.activity_id,T2.activity_id),
      COALESCE(T1.user_id,T2.user_id),
      COALESCE(T1.first_charge_amount,T2.first_charge_amount),
      COALESCE(T1.first_charge_time,T2.first_charge_time),
      COALESCE(T1.first_back_amount,T2.first_back_amount),
      COALESCE(T1.first_back_time,T2.first_back_time)
from (select * from yc_dm_mp.dm_user_first_charge where dt={dt_1day_ago}) T1
full outer join
(select t1.account_id as account_id,t1.activity_id as activity_id,t3.user_id as user_id,
t1.amount as first_charge_amount,t1.create_time as first_charge_time,
t2.amount as first_back_amount,t2.create_time as first_back_time
from
(select *,row_number() over(partition by account_id order by create_time)num1 from yc_bit.account_history where reason=1 and type in(2,4) and dt={dt})t1
left join
(select *,row_number() over(partition by account_id order by create_time)num2 from yc_bit.account_history where reason=16 and type in(2,4) and dt={dt})t2
on t1.account_id=t2.account_id and t2.num2=1
left join
(select user_id,account_id,row_number() over(partition by user_id order by create_time desc)num3 from yc_ods.fu_user where dt<={dt})t3
on t1.account_id=t3.account_id and t3.num3=1
where t1.num1=1)T2
on T1.account_id=T2.account_id
       """.format(dt=dt,dt_1day_ago=dt_1day_ago)
       print sql2
       ychive().query(sql2)
       start_time+=86400
