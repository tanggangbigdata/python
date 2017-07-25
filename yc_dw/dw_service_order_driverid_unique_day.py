#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-01"
#对订单表按照user_id去重，保留部分字段


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
       sql="""insert overwrite table yc_dw_mp.dw_service_order_driverid_unique_day partition(dt={dt})
       select service_order_id,
product_type_id,
fixed_product_id,
city,
create_time,
total_amount,
origin_amount,
sharing_amount,
status,
start_time,
end_time,
user_id,
corporate_id,
car_id,
driver_id,
driver_name,
driver_phone,
car_type_id,
car_type,
car_brand,
vehicle_number,
reason_id,
pay_amount,
is_auto_dispatch,
update_time,
origin_sharing_amount,
pay_status,
flag,
confirm_time,
arrival_time,
driver_night_amount,
highway_amount,
parking_amount,
is_night,
driver_service_amount,
predict_origin_amount,
predict_amount,
compute_amount,
car_type_ids,
platform,
account_id,
rc_status
       from
       (select *,row_number() over(partition by driver_id,status order by update_time desc)num from yc_ods.ods_service_order
       where dt={dt})t1
       where t1.num=1 
       """.format(dt=dt)
       ychive().query(sql)
       start_time+=86400

