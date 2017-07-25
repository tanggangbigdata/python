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
       dt_2day_ago = time.strftime('%Y%m%d', time.localtime(start_time_1day_ago-86400))
       sql1="""
       insert overwrite table yc_dm_mp.dm_service_order_all_unique partition(dt=20170510)
select service_order_id,
product_type_id,
fixed_product_id,
city,
create_time,
total_amount,
origin_amount,
sharing_amount,
status,
expect_start_time,
expect_end_time,
start_time,
end_time,
user_id,
corporate_id,
car_id,
driver_id,
driver_name,
driver_phone,
user_type,
car_type_id,
coupon_name,
coupon_type,
source,
coupon_facevalue,
dependable_distance,
actual_time_length,
time_length,
passenger_name,
is_asap,
car_type,
car_brand,
vehicle_number,
invoice,
start_position,
end_position,
coupon_member_id,
msg,
comment,
reason_id,
operator_id,
last_operator,
deposit,
pay_amount,
is_auto_dispatch,
app_msg,
regulatepan_amount,
regulatedri_amount,
update_time,
passenger_phone,
origin_sharing_amount,
pay_status,
flag,
confirm_time,
version,
app_version,
balance_status,
receipt_title,
arrival_time,
driver_night_amount,
highway_amount,
parking_amount,
is_night,
deadhead_distance,
extension,
driver_service_amount,
predict_origin_amount,
predict_amount,
start_latitude,
start_longitude,
compute_amount,
addons_amount,
expect_start_latitude,
expect_start_longitude,
expect_end_latitude,
expect_end_longitude,
system_distance,
mileage,
car_type_ids,
regulatepan_reason,
regulatedri_reason,
abnormal_mark,
runtime,
platform,
user_phone,
account_id,
rc_status,
corporate_dept_id
from
(select *,row_number() over(partition by service_order_id order by update_time desc)num
from yc_ods.ods_service_order where dt<=20170510)t1
where t1.num=1
       """
#       ychive().query(sql1)
       sql2="""
       insert overwrite table yc_dm_mp.dm_service_order_all_unique partition(dt={dt})
       select
COALESCE(t2.service_order_id,t1.service_order_id),
COALESCE(t2.product_type_id,t1.product_type_id),
COALESCE(t2.fixed_product_id,t1.fixed_product_id),
COALESCE(t2.city,t1.city),
COALESCE(t2.create_time,t1.create_time),
COALESCE(t2.total_amount,t1.total_amount),
COALESCE(t2.origin_amount,t1.origin_amount),
COALESCE(t2.sharing_amount,t1.sharing_amount),
COALESCE(t2.status,t1.status),
COALESCE(t2.expect_start_time,t1.expect_start_time),
COALESCE(t2.expect_end_time,t1.expect_end_time),
COALESCE(t2.start_time,t1.start_time),
COALESCE(t2.end_time,t1.end_time),
COALESCE(t2.user_id,t1.user_id),
COALESCE(t2.corporate_id,t1.corporate_id),
COALESCE(t2.car_id,t1.car_id),
COALESCE(t2.driver_id,t1.driver_id),
COALESCE(t2.driver_name,t1.driver_name),
COALESCE(t2.driver_phone,t1.driver_phone),
COALESCE(t2.user_type,t1.user_type),
COALESCE(t2.car_type_id,t1.car_type_id),
COALESCE(t2.coupon_name,t1.coupon_name),
COALESCE(t2.coupon_type,t1.coupon_type),
COALESCE(t2.source,t1.source),
COALESCE(t2.coupon_facevalue,t1.coupon_facevalue),
COALESCE(t2.dependable_distance,t1.dependable_distance),
COALESCE(t2.actual_time_length,t1.actual_time_length),
COALESCE(t2.time_length,t1.time_length),
COALESCE(t2.passenger_name,t1.passenger_name),
COALESCE(t2.is_asap,t1.is_asap),
COALESCE(t2.car_type,t1.car_type),
COALESCE(t2.car_brand,t1.car_brand),
COALESCE(t2.vehicle_number,t1.vehicle_number),
COALESCE(t2.invoice,t1.invoice),
COALESCE(t2.start_position,t1.start_position),
COALESCE(t2.end_position,t1.end_position),
COALESCE(t2.coupon_member_id,t1.coupon_member_id),
COALESCE(t2.msg,t1.msg),
COALESCE(t2.comment,t1.comment),
COALESCE(t2.reason_id,t1.reason_id),
COALESCE(t2.operator_id,t1.operator_id),
COALESCE(t2.last_operator,t1.last_operator),
COALESCE(t2.deposit,t1.deposit),
COALESCE(t2.pay_amount,t1.pay_amount),
COALESCE(t2.is_auto_dispatch,t1.is_auto_dispatch),
COALESCE(t2.app_msg,t1.app_msg),
COALESCE(t2.regulatepan_amount,t1.regulatepan_amount),
COALESCE(t2.regulatedri_amount,t1.regulatedri_amount),
COALESCE(t2.update_time,t1.update_time),
COALESCE(t2.passenger_phone,t1.passenger_phone),
COALESCE(t2.origin_sharing_amount,t1.origin_sharing_amount),
COALESCE(t2.pay_status,t1.pay_status),
COALESCE(t2.flag,t1.flag),
COALESCE(t2.confirm_time,t1.confirm_time),
COALESCE(t2.version,t1.version),
COALESCE(t2.app_version,t1.app_version),
COALESCE(t2.balance_status,t1.balance_status),
COALESCE(t2.receipt_title,t1.receipt_title),
COALESCE(t2.arrival_time,t1.arrival_time),
COALESCE(t2.driver_night_amount,t1.driver_night_amount),
COALESCE(t2.highway_amount,t1.highway_amount),
COALESCE(t2.parking_amount,t1.parking_amount),
COALESCE(t2.is_night,t1.is_night),
COALESCE(t2.deadhead_distance,t1.deadhead_distance),
COALESCE(t2.extension,t1.extension),
COALESCE(t2.driver_service_amount,t1.driver_service_amount),
COALESCE(t2.predict_origin_amount,t1.predict_origin_amount),
COALESCE(t2.predict_amount,t1.predict_amount),
COALESCE(t2.start_latitude,t1.start_latitude),
COALESCE(t2.start_longitude,t1.start_longitude),
COALESCE(t2.compute_amount,t1.compute_amount),
COALESCE(t2.addons_amount,t1.addons_amount),
COALESCE(t2.expect_start_latitude,t1.expect_start_latitude),
COALESCE(t2.expect_start_longitude,t1.expect_start_longitude),
COALESCE(t2.expect_end_latitude,t1.expect_end_latitude),
COALESCE(t2.expect_end_longitude,t1.expect_end_longitude),
COALESCE(t2.system_distance,t1.system_distance),
COALESCE(t2.mileage,t1.mileage),
COALESCE(t2.car_type_ids,t1.car_type_ids),
COALESCE(t2.regulatepan_reason,t1.regulatepan_reason),
COALESCE(t2.regulatedri_reason,t1.regulatedri_reason),
COALESCE(t2.abnormal_mark,t1.abnormal_mark),
COALESCE(t2.runtime,t1.runtime),
COALESCE(t2.platform,t1.platform),
COALESCE(t2.user_phone,t1.user_phone),
COALESCE(t2.account_id,t1.account_id),
COALESCE(t2.rc_status,t1.rc_status),
COALESCE(t2.corporate_dept_id,t1.corporate_dept_id)
from
(select * from yc_dm_mp.dm_service_order_all_unique where dt={dt_1day_ago})t1
full outer join
(select * from yc_ods.ods_service_order where dt={dt}) t2
on t1.service_order_id=t2.service_order_id """.format(dt_1day_ago=dt_1day_ago,dt=dt)
       ychive().query(sql2)
       sql3="""alter table yc_dm_mp.dm_service_order_all_unique drop partition(dt={dt})""".format(dt=dt_2day_ago)
       ychive().query(sql3)
       start_time+=86400
