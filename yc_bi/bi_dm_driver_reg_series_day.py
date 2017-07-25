#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-02"

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
        table = "bi_dm_driver_reg_series_day"
        dir = "/tmp/"+table
        hdfs_1 = "/user/hive/warehouse/yc_ods_mid_bi.db/bi_dm_driver_reg_series_day/"
        dt, dty, start_time, end_time = dateTime().getDay(argv)
        while start_time < end_time :
            start_time_1day_ago = start_time - 86400
            dt_day = time.strftime('%Y%m%d', time.localtime(start_time))
            hdfs = hdfs_1 + "dt=" + dt_day
            print hdfs
            dt = dtoop()
            sql = """insert overwrite table yc_ods_mid_bi.bi_dm_driver_reg_series_day partition(dt={dt_day})
            select count(driver_id_reg),
            count(driver_id_act_1day),
            count(driver_id_act_2day),
            count(driver_id_act_3day),
            count(driver_id_act_4day),
            count(driver_id_act_5day),
            count(driver_id_act_6day),
            count(driver_id_act_7day),
            count(driver_id_act_8day),
            count(driver_id_act_14day),
            count(driver_id_act_30day),
            count(driver_id_dispatch_1day),
            count(driver_id_dispatch_2day),
            count(driver_id_dispatch_3day),
            count(driver_id_dispatch_4day),
            count(driver_id_dispatch_5day),
            count(driver_id_dispatch_6day),
            count(driver_id_dispatch_7day),
            count(driver_id_dispatch_8day),
            count(driver_id_dispatch_14day),
            count(driver_id_dispatch_30day),
            count(driver_id_dispatch_accept_1day),
            count(driver_id_dispatch_accept_2day),
            count(driver_id_dispatch_accept_3day),
            count(driver_id_dispatch_accept_4day),
            count(driver_id_dispatch_accept_5day),
            count(driver_id_dispatch_accept_6day),
            count(driver_id_dispatch_accept_7day),
            count(driver_id_dispatch_accept_8day),
            count(driver_id_dispatch_accept_14day),
            count(driver_id_dispatch_accept_30day),
            count(driver_id_order_finish_1day),
            count(driver_id_order_finish_2day),
            count(driver_id_order_finish_3day),
            count(driver_id_order_finish_4day),
            count(driver_id_order_finish_5day),
            count(driver_id_order_finish_6day),
            count(driver_id_order_finish_7day),
            count(driver_id_order_finish_8day),
            count(driver_id_order_finish_14day),
            count(driver_id_order_finish_30day),
            dt
            from yc_dm_mp.dm_driver_reg_series_detail_info_day
            where dt={dt_day} group by dt
            """.format(dt_day=dt_day)
            print sql
            ychive().query(sql)

            try:
                dt.dtoopexport("to_54", table, dir, hdfs)
                os.remove(dir)
            except:
                sys.exit(1)
            start_time += 86400