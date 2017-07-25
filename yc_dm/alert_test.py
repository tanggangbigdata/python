#!/usr/bin/env python
#_*_ coding:utf-8 _*_
 
from core.alert import alert
from core.connmysql import connmysql
class table:
	def execute(self,argv):
	  alert().send_msg(["dm_service_order_create_time_static","count_num"],["dm_service_order_create_time_dynamic","count_num"],20170120,20170121)
