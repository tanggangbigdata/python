#!/usr/bin/env python
# _*_ coding:utf-8 _*_
__create_time__="2017-03-09"
__Author__ = "Tanggang"
__note__ = ""

from .cmd import cmd
from .conf import conf
from pyhive import hive
from TCLIService.ttypes import TOperationState

class ychive:

    cursor = None;

    def fetch(self):
        if self.cursor is not None :
            return self.cursor.fetchall()
        else :
            return False

    def query(self, sql, database="yc_bit", config="hive"):
        c = conf().getConf(config)

        cursor = hive.connect(c["host"], c["port"], database=database).cursor()
	print sql
        cursor.execute(sql, async=True)
	print "conn Success"
        status = cursor.poll().operationState
        while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
            logs = cursor.fetch_logs()
            for message in logs:
                print message

            # If needed, an asynchronous query can be cancelled at any time with:
            # cursor.cancel()

            status = cursor.poll().operationState

        self.cursor = cursor
        return self
