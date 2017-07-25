#!/usr/bin/env python
# _*_ coding:utf-8 _*_
__create_time__="2017-03-09"
__Author__ = "Tanggang"
__note__ = ""

import time,datetime

class dateTime:
	def getDay(self,argv):
		dty = time.strftime("%Y%m%d",time.localtime(int(time.time()) - 86400))
		dt = time.strftime("%Y%m%d",time.localtime(int(time.time())))
		if(len(argv) >2):
			dty = argv[2]
		start_time = int(time.mktime(time.strptime(dty,'%Y%m%d')))
		dt = time.strftime("%Y%m%d",time.localtime(start_time))
		end_time = start_time + 86400
		if(len(argv) >3):
			end_time = int(time.mktime(time.strptime(argv[3],'%Y%m%d')))

		return [dt,dty,start_time,end_time]
	def getHour(self,argv):
		dhy = time.strftime("%Y%m%d%H",time.localtime(int(time.time()) - 3600))
                dh = time.strftime("%Y%m%d%H",time.localtime(int(time.time())))
                if(len(argv) >2):
                        dhy = argv[2]
                start_time = int(time.mktime(time.strptime(dhy,'%Y%m%d%H')))
                dh = time.strftime("%Y%m%d%H",time.localtime(start_time))
                end_time = start_time + 3600
                if(len(argv) >3):
                        end_time = int(time.mktime(time.strptime(argv[3],'%Y%m%d%H')))
		return [dh,dhy,start_time,end_time]
