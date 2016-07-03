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
#   函数功能：获取线下训练阶段用户 id (U,L,M) 对应的类别标识
#
#   输入参数：time_start,time_end_ulm,time_end_feature
#
#   输出参数
#
#   编写时间：2016年5月
#
#####################################################################################################################
def Get_LabelTrain(id_ulm,time_end_ulm,time_end_feature):
    # 连接数据库
    conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',port=3306,db='db_koubei')
    cur = conn.cursor()

    # 构建 sql 语句
    # 获取在训练集上提取的类别 id
    sql_ulm = "select user_id,location_id,merchant_id from koubei_train\
           where Time_Stamp >= '%s' and\
                 Time_Stamp < '%s'\
           group by user_id,location_id,merchant_id;"%(time_end_feature,time_end_ulm)


    # 执行数据库操作
    execute = cur.execute(sql_ulm)
    results_ulm = cur.fetchmany(execute)

    ulm_dict = {}
    ulm_list = []
    for ulm in results_ulm:
        ulm__id = (ulm[0],ulm[1],ulm[2])
        ulm_dict[ulm__id] = 0
        ulm_list.append(ulm__id)


    Y = []
    for ulm in id_ulm:
        id_1 = (ulm[0],ulm[1],ulm[2])
        if ulm_dict.has_key(id_1):
            Y.append(1)
        else:
            Y.append(0)


    # 关闭数据库
    conn.commit()
    cur.close()
    conn.close()

    return Y,ulm_list



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
    
    time_start         = '2015-11-01 00:00:00'
    time_end_feature   = '2015-12-01 00:00:00'
    time_end_ulm       = '2015-12-01 00:00:00'

    # 获取训练阶段的 ULM
    id_ulm = Get_ULM_Train(time_start,time_end_ulm,time_end_feature)

    # 获取测试阶段的 ULM
    id_ulm = Get_ULM_Test(time_start,time_end_feature)
    print 'Get ULM over.'


    print 'Total time: ', time.time() - time_begin