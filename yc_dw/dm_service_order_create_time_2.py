#!/usr/bin/env python 
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-03-13"

from core.ychive import ychive
from core.dateTime import dateTime
from core.connmysql import connmysql
import time
import datetime
import sys,os
reload(sys)
sys.setdefaultencoding('utf-8')

class table:
  def execute(self,argv):
    dt,dty,start_time,end_time = dateTime().getDay(argv)
    while start_time<end_time:
	dt=time.strftime('%Y%m%d',time.localtime(start_time))
    	connmysql("188_mysql",database="hive_alert").insert("dw_service_order_group",20161113,20161213,0)
        result=connmysql("188_mysql",database="hive_alert").query("dw_service_order_group",20161113,'dt')
        print result
        start_time +=86400
