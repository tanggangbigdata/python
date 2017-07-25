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
       start_time_20day_ago = start_time-1728000
       dt = time.strftime('%Y%m%d',time.localtime(start_time))
       dt_1day_ago=time.strftime('%Y%m%d',time.localtime(start_time_1day_ago))
       dt_part = time.strftime('%Y%m%d',time.localtime(start_time_20day_ago))
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
        from (select *,row_number() over(partition by user_id order by update_time desc)num from yc_ods.fu_user where dt<=20170420)T1
        where T1.num=1
       """
       print sql1
#       ychive().query(sql1)
       sql2="""
    insert overwrite table yc_dw_mp.dw_user_base_info partition(dt={dt})
       select COALESCE(T2.user_id,T1.user_id),
      COALESCE(T2.cellphone,T1.cellphone),
      COALESCE(T2.type,T1.type),
      COALESCE(T2.name,T1.name),
      COALESCE(T2.gender,T1.gender),
      COALESCE(T2.head_image,T1.head_image),
      COALESCE(T2.head_image_thumb,T1.head_image_thumb),
      COALESCE(T2.birth_date,T1.birth_date),
      COALESCE(T2.birth_mmdd,T1.birth_mmdd),
      COALESCE(T2.mobile_region_code,T1.mobile_region_code),
      COALESCE(T2.country,T1.country),
      COALESCE(T2.city,T1.city),
      COALESCE(T2.status,T1.status),
      COALESCE(T2.limit_flag,T1.limit_flag),
      COALESCE(T2.flag,T1.flag),
      COALESCE(T2.app_id,T1.app_id),
      COALESCE(T2.account_id,T1.account_id),
      COALESCE(T2.app_ids,T1.app_ids),
      COALESCE(T2.letv_uid,T1.letv_uid),
      COALESCE(T2.create_time,T1.create_time),
      COALESCE(T2.update_time,T1.update_time),
      COALESCE(T2.mtime,T1.mtime)
      from (select * from yc_dw_mp.dw_user_base_info where dt={dt_1day_ago})T1
      full outer join
      (select * from
      (select *,row_number() over(partition by user_id order by update_time desc)num2 from yc_ods.fu_user where dt={dt})t2 where t2.num2=1)T2
      on T1.user_id=T2.user_id
       """.format(dt=dt,dt_1day_ago=dt_1day_ago)
       print sql2
       ychive().query(sql2)
       sql3="""alter table yc_dw_mp.dw_user_base_info drop partition(dt={dt_part})""".format(dt_part=dt_part)
       print sql3
       ychive().query(sql3)
       start_time+=86400

