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
      start_time_ago_30 = start_time-2592000
      start_time_next_30 = start_time+2592000
      dt = time.strftime('%Y%m%d',time.localtime(start_time))
#      dt_ago_30 = time.strftime('%Y%m%d',time.localtime(start_time_ago_30))
#      dt_next_30 = time.strftime('%Y%m%d', time.localtime(start_time_next_30))
      queue=Queue.Queue()
      queue2 = Queue.Queue()
      while start_time_ago_30<=start_time:
          dtn=time.strftime('%Y%m%d',time.localtime(start_time_ago_30))
          dtn_ago_30=time.strftime('%Y%m%d',time.localtime(start_time_ago_30-2592000))
          dtn_next_30=time.strftime('%Y%m%d',time.localtime(start_time_ago_30+2592000))
          argu=[]
          sql1="""insert overwrite table yc_dm_mp.dm_service_order_end_time partition (dt={dtn})
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
from yc_ods.ods_service_order where dt>={dtn_ago_30} and dt<={dtn_next_30} and from_unixtime(end_time,'yyyyMMdd')={dtn}
""".format(dtn=dtn,dtn_ago_30=dtn_ago_30,dtn_next_30=dtn_next_30)
#          print sql2
          sql2 = """select count(distinct service_order_id) from yc_dm_mp.dm_service_order_end_time
                     where dt>={dtn_ago_30} and dt<={dtn_next_30} and
                     from_unixtime(end_time,'yyyyMMdd')={dtn}""".format(dtn_ago_30=dtn_ago_30,dtn_next_30=dtn_next_30, dtn=dtn)
          argu.append((sql2,dtn))
          print argu
          queue2.put(argu)
          queue.put(sql1)
          start_time_ago_30 += 86400
      for i in range(10):
        t=Td(i,queue)
        t.setDaemon(True)
        t.start()
      queue.join()
      for j in range(10):
        s=Td_local(j,queue2,dt)
        s.setDaemon(True)
        s.start()
      queue2.join()
      start_time += 86400
      print "END SUCCESSFUL"
class Td_local(threading.Thread):
  def __init__(self,threadTD,queue2,dt):
         threading.Thread.__init__(self)
         self.threadID =threadTD
         self.queue=queue2
         self.dt=dt
  def run(self):
         while True:
#           sql = self.queue.get()[0][0]
#           print sql[0]
#           dtn = self.queue.get()[0][1]
           dt=self.dt
           try:
                tup=self.queue.get()[0]
                sql = tup[0]
                dtn = tup[1]
                print dtn
                print "\n"
                result=ychive().query(sql).fetch()
                connmysql("188_mysql",database="hive_alert").insert("dm_service_order_end_time_dynamic",dt,dtn,result[0][0])
           finally:
                self.queue.task_done()
