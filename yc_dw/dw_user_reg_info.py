#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-04-26"


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
        insert overwrite table yc_dw_mp.dw_user_reg_info partition(dt=20170420)
        select user_id,
        platform,
        source_1,
        source_2,
        source_3,
        source_4,
        source_5,
        invite_type,
        relative_user_id,
        create_operator_id,
        create_time,
        origin_cellphone,
        invite_code,
        ip,
        port,
        user_agent,
        uuid,
        macaddress,
        bluetooth_id,
        brand_id,
        mtime
        from (select *,row_number() over(partition by user_id order by mtime desc)num from fu_user_reg where dt<=20170420)T1
        where T1.num=1
       """
       print sql1
#       ychive().query(sql1)
       sql2="""
    insert overwrite table yc_dw_mp.dw_user_reg_info partition(dt={dt})
       select COALESCE(T2.user_id,T1.user_id),
      COALESCE(T2.platform,T1.platform),
      COALESCE(T2.source_1,T1.source_1),
      COALESCE(T2.source_2,T1.source_2),
      COALESCE(T2.source_3,T1.source_3),
      COALESCE(T2.source_4,T1.source_4),
      COALESCE(T2.source_5,T1.source_5),
      COALESCE(T2.invite_type,T1.invite_type),
      COALESCE(T2.relative_user_id,T1.relative_user_id),
      COALESCE(T2.create_operator_id,T1.create_operator_id),
      COALESCE(T2.create_time,T1.create_time),
      COALESCE(T2.origin_cellphone,T1.origin_cellphone),
      COALESCE(T2.invite_code,T1.invite_code),
      COALESCE(T2.ip,T1.ip),
      COALESCE(T2.port,T1.port),
      COALESCE(T2.user_agent,T1.user_agent),
      COALESCE(T2.uuid,T1.uuid),
      COALESCE(T2.macaddress,T1.macaddress),
      COALESCE(T2.bluetooth_id,T1.bluetooth_id),
      COALESCE(T2.brand_id,T1.brand_id),
      COALESCE(T2.mtime,T1.mtime)
      from (select * from yc_dw_mp.dw_user_reg_info where dt={dt_1day_ago})T1
      full outer join
      (select * from
      (select *,row_number() over(partition by user_id order by mtime desc)num2 from yc_ods.fu_user_reg where dt={dt})t2 where t2.num2=1)T2
      on T1.user_id=T2.user_id
       """.format(dt=dt,dt_1day_ago=dt_1day_ago)
       print sql2
       ychive().query(sql2)
       start_time+=86400

