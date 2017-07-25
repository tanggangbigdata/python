#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-07"

from core.dateTime import dateTime
from core.dtoop import dtoop
from core.ychive import ychive
import time
import datetime
from core.connmysql import connmysql
import os
import sys

# [HIVE] yc_ods_mid_bi.bi_dm_driver_reg_series_day -> [mysql] yc_bit.ods_bound



class table:
    def execute(self, argv):
        table = "bi_dm_driver_reg_series_week"
        dir = "/tmp/"+table
        hdfs_1 = "/user/hive/warehouse/yc_ods_mid_bi.db/bi_dm_driver_reg_series_week/"
        dt, dty, start_time, end_time = dateTime().getDay(argv)
        while start_time < end_time :
            start_time_1day_ago = start_time - 86400
            dt_day = time.strftime('%Y%m%d', time.localtime(start_time))
            hdfs = hdfs_1 + "dt=" + dt_day
            print hdfs
            dt = dtoop()
            sql = """insert overwrite table yc_ods_mid_bi.bi_dm_driver_reg_series_week partition(dt={dt_day})
            select count(driver_id_reg_week),
            count(driver_id_act_1week),
            count(driver_id_act_2week),
            count(driver_id_act_3week),
            count(driver_id_act_4week),
            count(driver_id_act_5week),
            count(driver_id_act_6week),
            count(driver_id_act_7week),
            count(driver_id_act_8week),
            count(driver_id_act_9week),
            count(driver_id_act_10week),
            count(driver_id_dispatch_1week),
            count(driver_id_dispatch_2week),
            count(driver_id_dispatch_3week),
            count(driver_id_dispatch_4week),
            count(driver_id_dispatch_5week),
            count(driver_id_dispatch_6week),
            count(driver_id_dispatch_7week),
            count(driver_id_dispatch_8week),
            count(driver_id_dispatch_9week),
            count(driver_id_dispatch_10week),
            count(driver_id_dispatch_accept_1week),
            count(driver_id_dispatch_accept_2week),
            count(driver_id_dispatch_accept_3week),
            count(driver_id_dispatch_accept_4week),
            count(driver_id_dispatch_accept_5week),
            count(driver_id_dispatch_accept_6week),
            count(driver_id_dispatch_accept_7week),
            count(driver_id_dispatch_accept_8week),
            count(driver_id_dispatch_accept_9week),
            count(driver_id_dispatch_accept_10week),
            count(driver_id_order_finish_1week),
            count(driver_id_order_finish_2week),
            count(driver_id_order_finish_3week),
            count(driver_id_order_finish_4week),
            count(driver_id_order_finish_5week),
            count(driver_id_order_finish_6week),
            count(driver_id_order_finish_7week),
            count(driver_id_order_finish_8week),
            count(driver_id_order_finish_9week),
            count(driver_id_order_finish_10week),
            dt
            from yc_dm_mp.dm_driver_reg_series_detail_info_week
            where dt={dt_day} group by dt
            """.format(dt_day=dt_day)
            print sql
            ychive().query(sql)

            try:
                dt.dtoopexport("to_54", table, dir, hdfs)
                os.remove(dir)
            except:
                sys.exit(1)
            start_time += 604800