#!/usr/bin/env python 
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-03-13"

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
	print start_time,end_time
	dt=time.strftime('%Y%m%d',time.localtime(start_time))
        sql1="""select distinct(from_unixtime(create_time,'yyyyMMdd')) from yc_ods.ods_service_order where create_time is not null and  dt={dt}""".format(dt=dt)
        result= ychive().query(sql1).fetch()
   	queue=Queue.Queue()
        for i in result:
	  print i[0]
 	  connmysql("188_mysql",database="hive_alert").insert("dw_service_order_group",dt,i[0],0)
          sql2="""insert into table yc_dw_mp.dw_service_order_create_time partition (dt={dt1})
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
from yc_ods.ods_service_order where dt={dt} and create_time is not null and from_unixtime(create_time,'yyyyMMdd')={dt} 
""".format(dt=dt,dt1=i[0])
#          print sql2
#          ychive().query(sql2)
          queue.put(sql2)
        for i in range(10):
	  t=Td(i,queue)
	  t.setDaemon(True)
	  t.start()
	queue.join()
        start_time += 86400
        print "END SUCCESSFUL"
#class Td(threading.Thread):
#  def __init__(self,threadTD,queue):
#        threading.Thread.__init__(self)
#        self.queue=queue
#  def run(self):
#        while True:
#          sql=self.queue.get()
#	 print sql
#	  try:
#	    print 123
#	    ychive().query(sql)
#	  finally:
#	    self.queue.task_done()
#	ychive().query(sql)
#        start_time += 86400
#	print "HELLO WORLD"
