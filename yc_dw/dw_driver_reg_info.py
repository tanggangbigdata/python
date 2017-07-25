#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-05-23"


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
       start_time_20day_ago = start_time -1728000
       dt = time.strftime('%Y%m%d',time.localtime(start_time))
       dt_1day_ago=time.strftime('%Y%m%d',time.localtime(start_time_1day_ago))
       dt_part = time.strftime('%Y%m%d',time.localtime(start_time_20day_ago))
       sql1="""
        insert overwrite table yc_dw_mp.dw_driver_reg_info partition(dt=20170520)
        select driver_register_id,
        driver_id,
        company_id,
        cellphone,
        name,
        city,
        gender,
        age,
        identity_card,
        driver_license,
        license_start_date,
        vehicle_owner,
        vehicle_owner_name,
        car_brand_id,
        car_model_id,
        car_brand,
        car_color,
        seat_num,
        vehicle_number,
        car_register_time,
        photo_id,
        identity_img_id,
        driver_license_img_id,
        car_license_img_id,
        driver_img_id,
        insurance_img_id,
        business_insurance_img_id,
        info_source,
        channel_source,
        create_time,
        update_time,
        audit_time,
        audit_fail_reason,
        audit_log,
        audit_operator_id,
        audit_status,
        invite_code,
        register_ip,
        step,
        manual_audit
        from (select *,row_number() over(partition by driver_id order by update_time desc)num from yc_ods.ods_driver_register where dt<=20170520)T1
        where T1.num=1
       """
       print sql1
#       ychive().query(sql1)
       sql2="""
    insert overwrite table yc_dw_mp.dw_driver_reg_info partition(dt={dt})
       select COALESCE(T2.driver_register_id,T1.driver_register_id),
      COALESCE(T2.driver_id,T1.driver_id),
      COALESCE(T2.company_id,T1.company_id),
      COALESCE(T2.cellphone,T1.cellphone),
      COALESCE(T2.name,T1.name),
      COALESCE(T2.city,T1.city),
      COALESCE(T2.gender,T1.gender),
      COALESCE(T2.age,T1.age),
      COALESCE(T2.identity_card,T1.identity_card),
      COALESCE(T2.driver_license,T1.driver_license),
      COALESCE(T2.license_start_date,T1.license_start_date),
      COALESCE(T2.vehicle_owner,T1.vehicle_owner),
      COALESCE(T2.vehicle_owner_name,T1.vehicle_owner_name),
      COALESCE(T2.car_brand_id,T1.car_brand_id),
      COALESCE(T2.car_model_id,T1.car_model_id),
      COALESCE(T2.car_brand,T1.car_brand),
      COALESCE(T2.car_color,T1.car_color),
      COALESCE(T2.seat_num,T1.seat_num),
      COALESCE(T2.vehicle_number,T1.vehicle_number),
      COALESCE(T2.car_register_time,T1.car_register_time),
      COALESCE(T2.photo_id,T1.photo_id),
      COALESCE(T2.identity_img_id,T1.identity_img_id),
      COALESCE(T2.driver_license_img_id,T1.driver_license_img_id),
      COALESCE(T2.car_license_img_id,T1.car_license_img_id),
      COALESCE(T2.driver_img_id,T1.driver_img_id),
      COALESCE(T2.insurance_img_id,T1.insurance_img_id),
      COALESCE(T2.business_insurance_img_id,T1.business_insurance_img_id),
      COALESCE(T2.info_source,T1.info_source),
      COALESCE(T2.channel_source,T1.channel_source),
      COALESCE(T2.create_time,T1.create_time),
      COALESCE(T2.update_time,T1.update_time),
      COALESCE(T2.audit_time,T1.audit_time),
      COALESCE(T2.audit_fail_reason,T1.audit_fail_reason),
      COALESCE(T2.audit_log,T1.audit_log),
      COALESCE(T2.audit_operator_id,T1.audit_operator_id),
      COALESCE(T2.audit_status,T1.audit_status),
      COALESCE(T2.invite_code,T1.invite_code),
      COALESCE(T2.register_ip,T1.register_ip),
      COALESCE(T2.step,T1.step),
      COALESCE(T2.manual_audit,T1.manual_audit)
      from (select * from yc_dw_mp.dw_driver_reg_info where dt={dt_1day_ago})T1
      full outer join
      (select * from
      (select *,row_number() over(partition by driver_id order by update_time desc)num2 from yc_ods.ods_driver_register where dt={dt})t2 where t2.num2=1)T2
      on T1.driver_id=T2.driver_id
       """.format(dt=dt,dt_1day_ago=dt_1day_ago)
       print sql2
       ychive().query(sql2)
       sql3 = """alter table yc_dw_mp.dw_driver_reg_info drop partition(dt={dt_part})""".format(dt_part=dt_part)
       print sql3
       ychive().query(sql3)
       start_time+=86400

