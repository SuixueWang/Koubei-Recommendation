#!usr/bin/env python
#-*- coding: utf-8 -*-
__author__ = 'wangsuixue'

import time
import copy
import numpy as np
import MySQLdb
from scipy.stats.stats import pearsonr


#####################################################################################################################
#   函数功能：计算地理位置特征
#
#   输入参数：无
#
#   输出参数：无
#
#   编写时间：2016年5月
#
#####################################################################################################################
def LocationFeature(id_ulm,time_start,time_end_feature):

    # 连接数据库
    conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',port=3306,db='db_koubei')
    cur = conn.cursor()

    id_l = set(list(np.array(id_ulm)[:,1]))
    print 'The number of id_l: ',len(id_l)
    # 用逗号分隔开
    location_list = ",".join(map(str,list(id_l)))

    # ---------------------------- 地理位置线下特征 LT1 --------------------------------------------------------
    # 构建数据库语句，查找地理位置线下销量
    sql_uid = "select location_id,count(*) from koubei_train\
           where location_id in (%s) and\
                 time_stamp >= '%s' and\
                 time_stamp <  '%s' \
            group by location_id;"%(location_list,time_start,time_end_feature)

    execute = cur.execute(sql_uid)
    lid_off = cur.fetchmany(execute)

    loc_off = {}
    for uid in lid_off:
        loc_off[uid[0]] = uid[1]

    print 'The number of lid_off: ',len(lid_off)

    # ------------------------------ 地理位置线下特征 LT2 -----------------------------------------------------------
    # 构建数据库语句，查找地理位置线下实体店个数
    sql_uid = "select location_id,count(distinct merchant_id) from koubei_train\
           where location_id in (%s) and\
                 time_stamp >= '%s' and\
                 time_stamp <  '%s' \
            group by location_id;"%(location_list,time_start,time_end_feature)

    execute = cur.execute(sql_uid)
    lid_off_mer = cur.fetchmany(execute)

    loc_off_mer = {}
    for uid in lid_off_mer:
        loc_off_mer[uid[0]] = uid[1]

    print 'The number of lid_off_mer: ',len(lid_off_mer)



    # -------------------------------------------------- 提取地区特征 --------------------------=---------------------
    # 提取用户特征
    X_location = []
    for id in id_ulm:
        L = id[1]

        LT1  = loc_off.get(L,0)
        LT3  = loc_off_mer.get(L,0)

        X_location.append((LT1,LT2,LT3))

    # 关闭数据库
    conn.commit()
    cur.close()
    conn.close()

    return X_location

#####################################################################################################################
#   函数功能：主函数
#
#   输入参数：无
#
#   输出参数：无
#
#   编写时间：2016年5月
#
#####################################################################################################################
if __name__ == '__main__':
    time_begin = time.time()
    
    print 'Total time: ', time.time() - time_begin