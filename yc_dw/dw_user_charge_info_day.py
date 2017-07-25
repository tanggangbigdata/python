#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-12"


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
       dt = time.strftime('%Y%m%d',time.localtime(start_time))
       dt_1day_ago=time.strftime('%Y%m%d',time.localtime(start_time_1day_ago))
       sql1="""
        insert overwrite table yc_dw_mp.dw_user_charge_info_day partition(dt={dt})
        select t1.balance_record_id,
        t1.account_id,
        t1.balance_id,
        t1.type,
        t1.consumer_id,
        t1.amount,
        t1.create_time,
        t1.reason,
        t1.record_time,
        t1.merchant_id,
        t1.activity_id,
        t2.user_id
        from
        (select *,row_number() over(partition by account_id,type order by create_time desc)num from yc_ods.fa_balance_record
        where dt={dt} and reason=1 )t1
        left join 
        (select user_id,account_id from yc_dw_mp.dw_user_base_info where dt={dt_today})t2
        on t1.account_id=t2.account_id
        where t1.num=1
       """.format(dt=dt,dt_today=dt_today)
       print sql1
       ychive().query(sql1)
       start_time+=86400

