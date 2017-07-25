#!/usr/bin/env python
#_*_ coding:utf-8 _*_
__Author__ ="tanggang"
__create_time__="2017-06-12"

from core.dateTime import dateTime
from core.dtoop import dtoop
from core.ychive import ychive
import time
import datetime
from core.connmysql import connmysql
import os
import sys

# [HIVE] yc_ods_mid_bi.bi_dm_driver_reg_series_week -> [mysql] yc_bit.ods_bound



class table:
    def execute(self, argv):
        table = "bi_dm_user_act_series_week"
        dir = "/tmp/"+table
        hdfs_1 = "/user/hive/warehouse/yc_ods_mid_bi.db/bi_dm_user_act_series_week/"
        dt, dty, start_time, end_time = dateTime().getDay(argv)
        while start_time < end_time :
            start_time_1day_ago = start_time - 86400
            dt_day = time.strftime('%Y%m%d', time.localtime(start_time))
            hdfs = hdfs_1 + "dt=" + dt_day
            print hdfs
            dt = dtoop()
            sql = """insert overwrite table yc_ods_mid_bi.bi_dm_user_act_series_week partition(dt={dt_day})
            select count(user_id_act_week),
            count(user_id_order_1week),
            count(user_id_order_2week),
            count(user_id_order_3week),
            count(user_id_order_4week),
            count(user_id_order_5week),
            count(user_id_order_6week),
            count(user_id_order_7week),
            count(user_id_order_8week),
            count(user_id_order_9week),
            count(user_id_order_10week),
            count(user_id_order_finish_1week),
            count(user_id_order_finish_2week),
            count(user_id_order_finish_3week),
            count(user_id_order_finish_4week),
            count(user_id_order_finish_5week),
            count(user_id_order_finish_6week),
            count(user_id_order_finish_7week),
            count(user_id_order_finish_8week),
            count(user_id_order_finish_9week),
            count(user_id_order_finish_10week),
            count(user_id_charge_1week),
            count(user_id_charge_2week),
            count(user_id_charge_3week),
            count(user_id_charge_4week),
            count(user_id_charge_5week),
            count(user_id_charge_6week),
            count(user_id_charge_7week),
            count(user_id_charge_8week),
            count(user_id_charge_9week),
            count(user_id_charge_10week),
            dt
            from yc_dm_mp.dm_user_act_series_detail_info_week
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