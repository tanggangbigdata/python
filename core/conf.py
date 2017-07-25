#!/usr/bin/env python
# _*_ coding:utf-8 _*_
__create_time__="2017-03-09"
__Author__ = "Tanggang"
__note__ = ""

import sys,os

class conf:
	def getConf(self,conn):
		conf={
		      "hive":{
     			      "host":"hdp1.bi.bj2.yongche.com",
			      "username":"hive",
			      "password":"hive",
			      "database":"bi_metastore",
			      "port":10000},
		     "188_mysql":{
  				 "user":"tanggang",
				 "password":"tang2016",
				 "host":"172.17.1.188",
				 "database":"hive_alert"},
			 "to_54": {
				 "host": "127.0.0.1",
				 "username": "root",
				 "password": "qwouca,203adfa",
				 "database": "yc_bit",
				 "target_host": "172.17.0.54"
			}
		     }
		if conn != '':
			return conf[conn]
		else:
			return conf

