#!usr/bin/env python
#-*- coding: utf-8 -*-
__author__ = 'wangsuixue'

import time
import copy
import numpy as np
# from numpy import *
import MySQLdb
import pickle
import pandas as pd


#####################################################################################################################
#   函数功能：计算实体店特征
#
#   输入参数：无
#
#   输出参数：无
#
#   编写时间：2016年5月
#
#####################################################################################################################
def UnionFeature(id_ulm,time_start,time_end_feature):

    # 连接数据库
    conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',port=3306,db='db_koubei')
    cur = conn.cursor()

    id_u = set(list(np.array(id_ulm)[:,0]))
    id_l = set(list(np.array(id_ulm)[:,1]))
    id_m = set(list(np.array(id_ulm)[:,2]))

    # 用逗号分隔开
    user_list     = ",".join(map(str,list(id_u)))
    location_list = ",".join(map(str,list(id_l)))
    merchant_list = ",".join(map(str,list(id_m)))

    # ---------------------------- 联合特征 UNT1 --------------------------------------------------------
    # 构建数据库语句，查找用户 U 去过实体店 M 多少次
    sql_uid = "select user_id,merchant_id,count(*) from koubei_train\
           where user_id in (%s) and merchant_id in (%s) and \
                 time_stamp >= '%s' and\
                 time_stamp <  '%s' \
            group by user_id,merchant_id;"%(user_list,merchant_list,time_start,time_end_feature)

    execute = cur.execute(sql_uid)
    um_id_off = cur.fetchmany(execute)

    user_mer_off = {}
    for uid in um_id_off:
        um_id = (uid[0],uid[1])
        user_mer_off[um_id] = uid[2]

    print 'The number of um_id_off: ',len(um_id_off)

    # -------------------------- 联合特征 UNT2 -----------------------------------------------------------
    # 构建数据库语句，查找用户 U 去过地区 L 多少次
    sql_uid = "select user_id,location_id,count(*) from koubei_train\
           where user_id in (%s) and location_id in (%s) and\
                 time_stamp >= '%s' and\
                 time_stamp <  '%s' \
            group by user_id,location_id;"%(user_list,location_list,time_start,time_end_feature)

    execute = cur.execute(sql_uid)
    ul_id_off = cur.fetchmany(execute)

    user_loc_off = {}
    for uid in ul_id_off:
        ul_id = (uid[0],uid[1])
        user_loc_off[ul_id] = uid[2]

    print 'The number of ul_id_off: ',len(ul_id_off)

    # ------------------------------ 联合特征 UNT3/UTN4 -----------------------------------------------------------
    # 构建数据库语句，查找实体店 M 的销量及其在本地区 L 所占的百分比
    sql_sum = "select location_id,count(*) from koubei_train\
           where location_id in (%s) and\
                 time_stamp >= '%s' and\
                 time_stamp <  '%s' \
            group by location_id;"%(location_list,time_start,time_end_feature)

    execute = cur.execute(sql_sum)
    lid_off = cur.fetchmany(execute)

    loc_off = {}
    for uid in lid_off:
        loc_off[uid[0]] = uid[1]

    sql_uid = "select location_id,merchant_id,count(*) from koubei_train\
               where merchant_id in (%s) and location_id in (%s) and\
                     time_stamp >= '%s' and\
                     time_stamp <  '%s' \
               group by location_id,merchant_id;"%(merchant_list,location_list,time_start,time_end_feature)

    execute = cur.execute(sql_uid)
    lm_off_loc = cur.fetchmany(execute)

    loc_mer_off = {}
    loc_mer_off_ratio = {}
    for uid in lm_off_loc:
        lm_id = (uid[0],uid[1])
        loc_mer_off[lm_id]       = uid[2]
        loc_mer_off_ratio[lm_id] = 1.0 * uid[2] / loc_off[uid[0]]

    print 'The number of lm_off_loc: ',len(lm_off_loc)

    # -------------------------- 联合特征 UNT5 -----------------------------------------------------------
    # 构建数据库语句，查找用户 U 去过 L 地区 M 实体店多少次
    sql_uid = "select user_id,location_id,merchant_id,count(*) from koubei_train\
           where user_id in (%s) and location_id in (%s) and merchant_id in (%s) and\
                 time_stamp >= '%s' and\
                 time_stamp <  '%s' \
            group by user_id,location_id,merchant_id;"%(user_list,location_list,merchant_list,time_start,time_end_feature)

    execute = cur.execute(sql_uid)
    ulm_id_off = cur.fetchmany(execute)

    user_loc_mer_off = {}
    for uid in ulm_id_off:
        ulm_id = (uid[0],uid[1],uid[2])
        user_loc_mer_off[ulm_id] = uid[3]

    print 'The number of ulm_id_off: ',len(ulm_id_off)

    # -------------------------------------------------- 提取联合特征 --------------------------=---------------------
    # 提取用户特征
    X_union = []
    for id in id_ulm:

        M    = id[2]
        UM   = (id[0],id[2])
        UL   = (id[0],id[1])
        LM   = (id[1],id[2])
        ULM  = (id[0],id[1],id[2])

        UNT1   = user_mer_off.get(UM,0)
        UNT2   = user_loc_off.get(UL,0)
        UNT3   = loc_mer_off.get(LM,0)
        UNT4   = loc_mer_off_ratio.get(LM,0)
        UNT5   = user_loc_mer_off.get(ULM,0)
        X_union.append((UNT1,UNT2,UNT3,UNT4,UNT5))


    # 关闭数据库
    conn.commit()
    cur.close()
    conn.close()

    return X_union


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