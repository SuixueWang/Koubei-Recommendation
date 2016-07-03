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
#   函数功能：获取训练阶段用户 id (U,L,M)
#
#   输入参数：time_start,time_end_ulm,time_end_feature
#
#   输出参数：id_ulm,id_u
#
#   编写时间：2016年5月
#
#####################################################################################################################
def Get_ULM_Train(time_start,time_end_ulm,time_end_feature):
    # 连接数据库
    conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',port=3306,db='db_koubei')
    cur = conn.cursor()

    # 构建 sql 语句
    # 获取(L,M)的时间段为统计特征的时间段
    sql_lm = "select location_id,merchant_id from koubei_train\
           where Time_Stamp >= '%s' and\
                 Time_Stamp < '%s'\
           group by location_id,merchant_id;"%(time_start,time_end_feature)


    # 执行数据库操作
    execute = cur.execute(sql_lm)
    results_lm = cur.fetchmany(execute)

    # 把（L,M）存放于字典中，key = L, value = M
    dict_lm = {}
    for id_lm in results_lm:
        dict_lm.setdefault(id_lm[0],[]).append(id_lm[1])

    # 构建 sql 语句
    sql_ul = "select user_id,location_id from koubei_train\
           where Time_Stamp >= '%s' and\
                 Time_Stamp < '%s'\
           group by user_id,location_id;"%(time_end_feature,time_end_ulm)

    execute = cur.execute(sql_ul)
    results_ul = cur.fetchmany(execute)

    # 提取 (U,L,M)
    id_ulm = []
    Abandon = []
    for id_ul in results_ul:                                  # 遍历所有的 (U,L) 组合
        if dict_lm.has_key(id_ul[1]):                         # 如果 (U,L) 中的 L 存在于 (L,M) 中，则组合 (U,L,M)
            for id_m in dict_lm[id_ul[1]]:
                id_ulm.append((id_ul[0],id_ul[1],id_m))
        else:                                                 # 如果 (U,L) 中的 L 不存在于 (L,M) 中，则丢弃当前的 L
            Abandon.append(id_ul[1])
            # print 'Abandon the new location_id: ',id_ul[1]

    print 'The number of Abandon location_id: ',len(set(Abandon))
    print 'The number of id_ulm: ',len(id_ulm)


    # 关闭数据库
    conn.commit()
    cur.close()
    conn.close()

    return id_ulm



#####################################################################################################################
#   函数功能：获取测试阶段用户 id (U,L,M)
#
#   输入参数：time_start,time_end_ulm,time_end_feature
#
#   输出参数：id_ulm
#
#   编写时间：2016年5月
#
#####################################################################################################################
def Get_ULM_Test(time_start,time_end_feature):
    # 连接数据库
    conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',port=3306,db='db_koubei')
    cur = conn.cursor()

    # 构建 sql 语句
    # 获取(L,M)的时间段为统计特征的时间段
    sql_lm = "select location_id,merchant_id from koubei_train\
           where Time_Stamp >= '%s' and\
                 Time_Stamp < '%s'\
           group by location_id,merchant_id;"%(time_start,time_end_feature)


    # 执行数据库操作
    execute = cur.execute(sql_lm)
    results_lm = cur.fetchmany(execute)

    # 把（L,M）存放于字典中，key = L, value = M
    dict_lm = {}
    for id_lm in results_lm:
        dict_lm.setdefault(id_lm[0],[]).append(id_lm[1])

    sql_ul_test = "select user_id,location_id from koubei_test;"

    execute = cur.execute(sql_ul_test)
    results_ul_test = cur.fetchmany(execute)

    results_ul = []
    for id_ul in results_ul_test:
        results_ul.append((id_ul[0],id_ul[1]))

    print 'The number of results_ul: ',len(results_ul)

    # 提取 (U,L,M)
    id_ulm = []
    Abandon = []
    for id_ul in results_ul:                                  # 遍历所有的 (U,L) 组合
        if dict_lm.has_key(id_ul[1]):                         # 如果 (U,L) 中的 L 存在于 (L,M) 中，则组合 (U,L,M)
            for id_m in dict_lm[id_ul[1]]:
                id_ulm.append((id_ul[0],id_ul[1],id_m))
        else:                                                 # 如果 (U,L) 中的 L 不存在于 (L,M) 中，则丢弃当前的 L
            Abandon.append(id_ul[1])
            # print 'Abandon the new location_id: ',id_ul[1]

    print 'The number of Abandon location_id: ',len(set(Abandon))
    print 'The number of id_ulm: ',len(id_ulm)


    # 关闭数据库
    conn.commit()
    cur.close()
    conn.close()

    return id_ulm

####################################################################################################################
def getTrainUser():
    # 连接数据库
    conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',port=3306,db='db_koubei')
    cur = conn.cursor()

    # 构建 sql 语句,获取线下的用户
    sql_lm = "select distinct user_id from koubei_train\
           where Time_Stamp >=  '2015-11-01 00:00:00' and\
                 Time_Stamp  <   '2015-12-01 00:00:00';"

    # 执行数据库操作
    execute = cur.execute(sql_lm)
    user_offline = cur.fetchmany(execute)


    # 构建 sql 语句,获取线上的用户
    sql_lm = "select distinct user_id from taobao\
           where Time_Stamp >=  '2015-11-01 00:00:00' and\
                 Time_Stamp  <   '2015-12-01 00:00:00';"

    # 执行数据库操作
    execute = cur.execute(sql_lm)
    user_online = cur.fetchmany(execute)

    user_train = {}
    for uid_off in user_offline:
        if ~user_train.has_key(uid_off):
            user_train[uid_off] = 1

    for uid_on in user_online:
        if ~user_train.has_key(uid_on):
            user_train[uid_on] = 1

    # 把 user_train 保存与 pickle 模块中
    file = open('user_train.pkl','w')
    pickle.dump(user_train,file)
    file.close()

    # 关闭数据库
    conn.commit()
    cur.close()
    conn.close()


#####################################################################################################################
#   函数功能：测试函数
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
    print 'Strating...'
    
    # time_start         = '2015-11-01 00:00:00'
    # time_end_feature   = '2015-12-01 00:00:00'
    # time_end_ulm       = '2015-12-01 00:00:00'
    #
    # # 获取训练阶段的 ULM
    # id_ulm = Get_ULM_Train(time_start,time_end_ulm,time_end_feature)
    #
    # # 获取测试阶段的 ULM
    # id_ulm = Get_ULM_Test(time_start,time_end_feature)
    # print 'Get ULM over.'

    # getTrainUser()

    print 'Total time: ', time.time() - time_begin