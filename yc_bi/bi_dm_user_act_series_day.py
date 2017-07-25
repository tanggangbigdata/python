#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-05"

from core.dateTime import dateTime
from core.dtoop import dtoop
from core.ychive import ychive
import time
import datetime
from core.connmysql import connmysql
import os
import sys

# [HIVE] yc_ods_mid_bi.bi_dm_driver_act_series_day -> [mysql] yc_bit.ods_bound



class table:
    def execute(self, argv):
        table = "bi_dm_user_act_series_day"
        dir = "/tmp/"+table
        hdfs_1 = "/user/hive/warehouse/yc_ods_mid_bi.db/bi_dm_user_act_series_day/"
        dt, dty, start_time, end_time = dateTime().getDay(argv)
        while start_time < end_time :
            start_time_1day_ago = start_time - 86400
            dt_day = time.strftime('%Y%m%d', time.localtime(start_time))
            hdfs = hdfs_1 + "dt=" + dt_day
            dt = dtoop()
            sql = """insert overwrite table yc_ods_mid_bi.bi_dm_user_act_series_day partition(dt={dt_day})
            select count(user_id_act),
            count(user_id_order_1day),
            count(user_id_order_2day),
            count(user_id_order_3day),
            count(user_id_order_4day),
            count(user_id_order_5day),
            count(user_id_order_6day),
            count(user_id_order_7day),
            count(user_id_order_8day),
            count(user_id_order_14day),
            count(user_id_order_30day),
            count(user_id_order_finish_1day),
            count(user_id_order_finish_2day),
            count(user_id_order_finish_3day),
            count(user_id_order_finish_4day),
            count(user_id_order_finish_5day),
            count(user_id_order_finish_6day),
            count(user_id_order_finish_7day),
            count(user_id_order_finish_8day),
            count(user_id_order_finish_14day),
            count(user_id_order_finish_30day),
            count(user_id_charge_1day),
            count(user_id_charge_2day),
            count(user_id_charge_3day),
            count(user_id_charge_4day),
            count(user_id_charge_5day),
            count(user_id_charge_6day),
            count(user_id_charge_7day),
            count(user_id_charge_8day),
            count(user_id_charge_14day),
            count(user_id_charge_30day),
            dt
            from yc_dm_mp.dm_user_act_series_detail_info_day
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