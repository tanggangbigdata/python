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
        insert overwrite table yc_dw_mp.dw_user_base_info partition(dt=20170420)
        select user_id,
        cellphone,
        type,
        name,
        gender,
        head_image,
        head_image_thumb,
        birth_date,
        birth_mmdd,
        mobile_region_code,
        country,
        city,
        status,
        limit_flag,
        flag,
        app_id,
        account_id,
        app_ids,
        letv_uid,
        create_time,
        update_time,
        mtime
        from (select *,row_number() over(partition by user_id order by update_time desc)num from fu_user where dt<=20170420)T1
        where T1.num=1
       """
       print sql1
#       ychive().query(sql1)
       sql2="""
    insert overwrite table yc_dw_mp.dw_user_base_info partition(dt={dt})
       select COALESCE(T1.user_id,T2.user_id),
      COALESCE(T1.cellphone,T2.cellphone),
      COALESCE(T1.type,T2.type),
      COALESCE(T1.name,T2.name),
      COALESCE(T1.gender,T2.gender),
      COALESCE(T1.head_image,T2.head_image),
      COALESCE(T1.head_image_thumb,T2.head_image_thumb),
      COALESCE(T1.birth_date,T2.birth_date),
      COALESCE(T1.birth_mmdd,T2.birth_mmdd),
      COALESCE(T1.mobile_region_code,T2.mobile_region_code),
      COALESCE(T1.country,T2.country),
      COALESCE(T1.city,T2.city),
      COALESCE(T1.status,T2.status),
      COALESCE(T1.limit_flag,T2.limit_flag),
      COALESCE(T1.flag,T2.flag),
      COALESCE(T1.app_id,T2.app_id),
      COALESCE(T1.account_id,T2.account_id),
      COALESCE(T1.app_ids,T2.app_ids),
      COALESCE(T1.letv_uid,T2.letv_uid),
      COALESCE(T1.create_time,T2.create_time),
      COALESCE(T1.update_time,T2.update_time),
      COALESCE(T1.mtime,T2.mtime)
      from (select * from yc_dw_mp.dw_user_base_info where dt={dt_1day_ago})T1
      full outer join
      (select * from
      (select *,row_number() over(partition by user_id order by update_time desc)num2 from yc_ods.fu_user where dt={dt})t2 where t2.num2=1)T2
      on T1.user_id=T2.user_id
       """.format(dt=dt,dt_1day_ago=dt_1day_ago)
       print sql2
       ychive().query(sql2)
       start_time+=86400

