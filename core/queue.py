#!/usr/bin/env python
# _*_ coding:utf-8 _*_
__create_time__="2017-03-09"
__Author__ = "Tanggang"
__note__ = ""

from ychive import ychive
from connmysql import connmysql
import threading
import Queue

class Td(threading.Thread):
	def __init__(self,threadID,queue):
	  threading.Thread.__init__(self)
	  self.thredID=threadID
	  self.queue=queue
	def run(self):
	  while True:
	    sql=self.queue.get()
	    print sql
	    try:
#		print 123
		ychive().query(sql)
	    finally:
		self.queue.task_done()
