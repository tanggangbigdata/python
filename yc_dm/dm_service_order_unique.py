#!/usr/bin/env python 
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-03-13"

from core.ychive import ychive
from core.dateTime import dateTime
from core.connmysql import connmysql
from core.queue import Td
from core.alert import alert
import time
import datetime
import sys,os
import Queue
import threading
reload(sys)
sys.setdefaultencoding('utf-8')

class table:
  def execute(self,argv):
    dt,dty,start_time,end_time = dateTime().getDay(argv)
    while start_time<end_time:
        dt=time.strftime('%Y%m%d',time.localtime(start_time))
        queue=Queue.Queue()
        queue2=Queue.Queue()
        result=connmysql("188_mysql",database="hive_alert").query("dw_service_order_group",dt,'dt_group')
        print result
        for i in result:
            print i[0]
            sql1="""
	      insert overwrite table yc_dm_mp.dm_service_order_unique partition (dt={dt})
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
from yc_dw_mp.dw_service_order_create_time where dt={dt})t1
where  t1.num=1
""".format(dt=i[0])
#          ychive().query(sql1)
	    queue.put(sql1)
        for z in range(10):
	    t=Td(z,queue)
	    t.setDaemon(True)
	    t.start()
	queue.join()
	sql2="select count(*) from yc_dm_mp.dm_service_order_unique where dt={dt}".format(dt=dt)
	result1= ychive().query(sql2).fetch()
	connmysql("188_mysql",database="hive_alert").insert("dm_service_order_create_time_static",dt,result1[0][0],0)
        for i in result:
            sql3="select count(*) from yc_dm_mp.dm_service_order_unique where dt={dt}".format(dt=i[0])
#            print sql3
            queue2.put(sql3)
        for j in result:
            t=Td_local(j[0],queue2,dt,j[0])
            t.setDaemon(True)
            t.start()
        queue2.join()
        start_time +=86400
        print "END SUCCESSFULL"
class Td_local(threading.Thread):
    def __init__(self,threadID,queue,dt1,dt2):
        threading.Thread.__init__(self)
	self.threadID=threadID
	self.queue=queue
	self.dt1=dt1
	self.dt2=dt2
    def run(self):
        while True:
          sql=self.queue.get()
          dt1=self.dt1
          dt2=self.dt2
          try:
            print sql
            result = ychive().query(sql).fetch()
            print result
            connmysql("188_mysql",database="hive_alert").insert("dm_service_order_create_time_dynamic",dt1,dt2,result[0][0])
            alert().send_msg(["dm_service_order_create_time_static","count_num"],["dm_service_order_create_time_dynamic","count_num"],dt1,dt2)
          finally:
            self.queue.task_done()
