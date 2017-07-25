#!/usr/bin/env python
# _*_ coding:utf-8 _*_
__create_time__="2017-03-14"
__Author__ = "Tanggang"
__note__ = ""

import commands
import time
import urllib2
import sys,os
from connmysql import connmysql
reload(sys)
sys.setdefaultencoding('utf-8')
global number
number=[15801103132,]
class alert:
	def send_msg(self,table1,table2,dt,dt_group):
	  countnum1=connmysql("188_mysql",database="hive_alert").query(table1[0],dt_group,table1[1])
	  countnum2=connmysql("188_mysql",database="hive_alert").query2(table2[0],dt,dt_group,table2[1])
	  print countnum1[0]
	  print countnum2
#	  differ=1
	  differ=countnum2[0][0]-countnum1[0][0]
          if differ !=0:
            msg="{dt}{table2}与{table1}的差异为:{num}".format(dt=dt,table1=table1[0],table2=table2[0],num=differ).replace(' ','%20')
            h="http://172.17.1.187:28080/sms"
	    for phone in number:
	      print number
 	      d="phone={phone}&msg={msg}".format(phone=phone, msg=msg)
	      try:
		urllib2.urlopen(h, d)
              except Exception,e:
                print '[ERROR]In send message: '+ e
 	def send_voice(self,table1,table2,dt):
          countnum1=connmysql("188_mysql",database="hive_alert").query(table1[0],dt,table1[1])
          countnum2=connmysql("188_mysql",database="hive_alert").query(table2[0],dt,table2[1])
          differ=self.countnum2-self.countnum1 
	  msg=123456
	  if differ !=0:
            h = "http://172.17.1.187:28080/voice"
   	    for phone in number:
	      d = "phone={phone}&msg={msg}".format(phone=phone, msg=msg)
	      try:
                urllib2.urlopen(h, d)
              except Exception,e:
                print '[ERROR]In send message: '+ e
