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
from GetULM import *


#####################################################################################################################
#   函数功能：统计线下实体店去过的用户，他们在线上购买了72种品牌的总次数
#
#   输入参数：无
#
#   输出参数：
#
#   编写时间：2016年5月
#
#####################################################################################################################
def OffMerchant_OnlineCategoryFeature(id_ulm,time_start,time_end_feature):

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

    # ------------------------------- 查找线上用户购买每件品牌的数量 ------------------------------------------------
    sql_user_cat =  "select user_id,category_id,count(*) from taobao\
                where user_id in (%s) and \
                      online_action_id = '1' and\
                      Time_Stamp >= '%s' and\
                      Time_Stamp < '%s'\
                group by user_id,category_id;"%(user_list,time_start,time_end_feature)
    execute = cur.execute(sql_user_cat)
    results_user_cat = cur.fetchmany(execute)

    print 'Query each category over.'

    user_cat_buy = {}
    for uc_id in results_user_cat:
        user_cat_buy.setdefault(uc_id[0],[0 for i in range(72)])[ int( uc_id[1] ) - 1 ] = uc_id[2]

    print len(user_cat_buy)

    # ------------------------------- 查找线上用户点击每件品牌的数量 ------------------------------------------------
    sql_user_cat =  "select user_id,category_id,count(*) from taobao\
                where user_id in (%s) and \
                      online_action_id = '0' and\
                      Time_Stamp >= '%s' and\
                      Time_Stamp < '%s'\
                group by user_id,category_id;"%(user_list,time_start,time_end_feature)
    execute = cur.execute(sql_user_cat)
    results_user_cat = cur.fetchmany(execute)

    print 'Query each category over.'

    user_cat_click = {}
    for uc_id in results_user_cat:
        user_cat_click.setdefault(uc_id[0],[0 for i in range(72)])[ int( uc_id[1] ) - 1 ] = uc_id[2]

    print len(user_cat_click)

    # --------------------------- 线下L地区M实体店的访问用户，分别在线上点击/购买品牌的次数 OffONT1/OffONT2 ----------------------------------
    # 构建数据库语句，查找线下L地区M实体店都有哪些用户光顾，且光顾了几次
    sql_mert_user =  "select location_id,merchant_id,user_id,count(*) from koubei_train\
                where Time_Stamp >= '%s' and\
                      Time_Stamp < '%s'\
                group by location_id,merchant_id,user_id;"%(time_start,time_end_feature)

    execute = cur.execute(sql_mert_user)
    results_LocMert_user = cur.fetchmany(execute)

    print 'The number of results_LocMert_user',len(results_LocMert_user)
    lm_buy_cat_times = {}
    lm_click_cat_times = {}
    lm_times_sum = {}
    T1 = 0
    for id_lm_u in results_LocMert_user:
        # print 'T1 = ',T1
        T1 += 1
        id_lm = (id_lm_u[0],id_lm_u[1])
        if lm_buy_cat_times.has_key(id_lm):
            lm_click_cat_times[id_lm] += np.array( user_cat_click.get(id_lm_u[2], [0 for i in range(72)]) ) * id_lm_u[3]
            lm_buy_cat_times[id_lm] += np.array( user_cat_buy.get(id_lm_u[2], [0 for i in range(72)]) ) * id_lm_u[3]
            lm_times_sum[id_lm] += id_lm_u[3]
        else:
            lm_click_cat_times[id_lm] =  np.array( user_cat_click.get(id_lm_u[2], [0 for i in range(72)]) ) * id_lm_u[3]
            lm_buy_cat_times[id_lm] =  np.array( user_cat_buy.get(id_lm_u[2], [0 for i in range(72)]) ) * id_lm_u[3]
            lm_times_sum[id_lm] = id_lm_u[3]


    # --------------------------------- 线下M实体店的访问用户，分别在线上点击/购买品牌的次数 OffONT3/OffONT4 ---------------=---------------------
    # 构建数据库语句，查找线下M实体店都有哪些用户光顾，且光顾了几次
    sql_mert_user =  "select merchant_id,user_id,count(*) from koubei_train\
                where Time_Stamp >= '%s' and\
                      Time_Stamp < '%s'\
                group by merchant_id,user_id;"%(time_start,time_end_feature)

    execute = cur.execute(sql_mert_user)
    results_Mert_user = cur.fetchmany(execute)

    print 'The number of results_Mert_user',len(results_Mert_user)
    m_buy_times = {}
    m_click_times = {}
    m_times_sum = {}
    T2 = 0
    for id_m_u in results_Mert_user:
        # print 'T2 = ',T2
        T2 += 1
        id_m = id_m_u[0]
        if m_buy_times.has_key(id_m):
            m_click_times[id_m] += np.array( user_cat_click.get(id_m_u[1], [0 for i in range(72)]) ) * id_m_u[2]
            m_buy_times[id_m] += np.array( user_cat_buy.get(id_m_u[1], [0 for i in range(72)]) ) * id_m_u[2]
            m_times_sum[id_m] += id_m_u[2]
        else:
            m_click_times[id_m] =  np.array( user_cat_click.get(id_m_u[1], [0 for i in range(72)]) ) * id_m_u[2]
            m_buy_times[id_m] =  np.array( user_cat_buy.get(id_m_u[1], [0 for i in range(72)]) ) * id_m_u[2]
            m_times_sum[id_m] = id_m_u[2]


    # ---------------------------------------------- 提取用户特征 -----------------------------------------------------
    # 提取用户特征
    X_Off_mer_Online_cat = []
    OFFONT_matrix = np.zeros((len(id_ulm),72*4))
    i = 0
    for id in id_ulm:

        LM   = (id[1],id[2])
        M    = id[2]

        OFFONT1  = 1.0 * lm_buy_cat_times[LM]/lm_times_sum[LM]
        OFFONT2  = 1.0 * lm_click_cat_times[LM]/lm_times_sum[LM]
        OFFONT3  = 1.0 * m_buy_times[M]/m_times_sum[M]
        OFFONT4  = 1.0 * m_click_times[M]/m_times_sum[M]

        OFFONT_matrix[i,0:72]     = OFFONT1
        OFFONT_matrix[i,72:144]   = OFFONT2
        OFFONT_matrix[i,144:216]  = OFFONT3
        OFFONT_matrix[i,216:288]  = OFFONT4

        i += 1
        # print 'i = ',i

    print np.shape(OFFONT_matrix)



    # ---------------------------------------------- PCA 降维 -----------------------------------------------------
    from sklearn import decomposition

    pca = decomposition.PCA()

    pca.n_components = 10
    X_reduced = pca.fit_transform(OFFONT_matrix)

    print X_reduced.shape


    # 关闭数据库
    conn.commit()
    cur.close()
    conn.close()

    return list(X_reduced)


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

    # 设定查询的时间段
    time_start         = '2015-07-01 00:00:00'
    time_end_feature   = '2015-08-01 00:00:00'
    time_end_ulm       = '2015-09-01 00:00:00'

    # 获取 ULM
    # id_ulm = Get_ULM_Test(time_start,time_end_feature)
    id_ulm = Get_ULM_Train(time_start,time_end_ulm,time_end_feature)
    X_Off_mer_Online_cat = OffMerchant_OnlineCategoryFeature(id_ulm,time_start,time_end_feature)

    print 'Total time: ', time.time() - time_begin