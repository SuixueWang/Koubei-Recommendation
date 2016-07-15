#!usr/bin/env python
#-*- coding: utf-8 -*-
__author__ = 'wangsuixue'

'''
程序功能: 作为单独的样本提取程序,分别提取线下和线上的数据集
'''

# 导包
import time
import numpy as np
import sys,os
sys.path.append('F:/MachineLearning/StoreRec/source/Koubei_V3/fcn_feature')

import pandas as pd
from GetULM import *
from UserFeature import *
from MerchantFeature import *
from LocationFeature import *
from UnionFeature import *
from GetLabel import *

#####################################################################################################################
#   函数功能：获取线下的训练或测试数据集
#
#   输入参数：time_start,time_end_ulm,time_end_feature
#
#   输出参数：X,Y,id_ulm,ulm_list
#
#   编写时间：2016年5月
#
#####################################################################################################################
def GetOffDataSet(time_start,time_end_ulm,time_end_feature):

    # 获取 ULM
    id_ulm = Get_ULM_Train(time_start,time_end_ulm,time_end_feature)
    print 'Get ULM over.'

    X_user,U_pearson_pvalue = CalUserFeature(id_ulm,time_start,time_end_feature)
    print np.shape(X_user)

    X_merchant,M_pearson_pvalue = MerchantFeature(id_ulm,time_start,time_end_feature)
    print np.shape(X_merchant)

    X_location,L_pearson_pvalue = LocationFeature(id_ulm,time_start,time_end_feature)
    print np.shape(X_location)

    X_union = UnionFeature(id_ulm,time_start,time_end_feature)

    print np.shape(X_union)

    X = np.c_[X_user,U_pearson_pvalue,X_merchant,M_pearson_pvalue,X_location,L_pearson_pvalue,X_union]
    X = list(X)

    Y,ulm_list = Get_LabelTrain(id_ulm,time_end_ulm,time_end_feature)

    return X,Y,id_ulm,ulm_list


#####################################################################################################################
#   函数功能：获取线上的特征集
#
#   输入参数：time_start,time_end_feature
#
#   输出参数：X,id_ulm
#
#   编写时间：2016年5月
#
#####################################################################################################################
def GetOnlineDataSet(time_start,time_end_feature):

    # 获取 ULM
    id_ulm = Get_ULM_Test(time_start,time_end_feature)
    print 'Get ULM over.'

    X_user,U_pearson_pvalue = CalUserFeature(id_ulm,time_start,time_end_feature)
    print np.shape(X_user)

    X_merchant,M_pearson_pvalue = MerchantFeature(id_ulm,time_start,time_end_feature)
    print np.shape(X_merchant)

    X_location,L_pearson_pvalue = LocationFeature(id_ulm,time_start,time_end_feature)
    print np.shape(X_location)

    X_union = UnionFeature(id_ulm,time_start,time_end_feature)
    print np.shape(X_union)

    X = np.c_[X_user,U_pearson_pvalue,X_merchant,M_pearson_pvalue,X_location,L_pearson_pvalue,X_union]
    X = list(X)

    return X,id_ulm

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
    print 'Starting...'

    # -------------------------------- 设定查询时间段 --------------------------------------------------
    time_7_1  = '2015-07-01 00:00:00'
    time_8_1  = '2015-08-01 00:00:00'
    time_9_1  = '2015-09-01 00:00:00'
    time_10_1 = '2015-10-01 00:00:00'
    time_11_1 = '2015-11-01 00:00:00'
    time_12_1 = '2015-12-01 00:00:00'

    time_7_15  = '2015-07-15 00:00:00'
    time_8_15  = '2015-08-15 00:00:00'
    time_9_15  = '2015-09-15 00:00:00'
    time_10_15 = '2015-10-15 00:00:00'
    time_11_15 = '2015-11-15 00:00:00'

    # -------------------------------- 提取线下训练数据集 X0,Y0-------------------------------------------
    X0,Y0,id_ulm_temp,ulm_list_temp = GetOffDataSet(time_7_1,time_9_1,time_8_1)
    del id_ulm_temp,ulm_list_temp
    print 'Get X0,Y0 over.'

    # -------------------------------- 提取线下测试数据集 X1,Y1 --------------------------------------
    X1,Y1,id_ulm_temp,ulm_list_temp = GetOffDataSet(time_8_1,time_10_1,time_9_1)
    X0.extend(X1)
    Y0.extend(Y1)
    del X1,Y1,id_ulm_temp,ulm_list_temp
    print 'Get X1,Y1 over.'
    
    # -------------------------------- 提取线下测试数据集 X2,Y2 --------------------------------------------
    
    X2,Y2,id_ulm_temp,ulm_list_temp = GetOffDataSet(time_9_1,time_11_1,time_10_1)
    X0.extend(X2)
    Y0.extend(Y2)
    del X2,Y2,id_ulm_temp,ulm_list_temp
    print 'Get X2,Y2 over.'
    
    # -------------------------------- 提取线下测试数据集 X3,Y3 --------------------------------------------
    X3,Y3,id_ulm_temp,ulm_list_temp = GetOffDataSet(time_10_1,time_12_1,time_11_1)
    X0.extend(X3)
    Y0.extend(Y3)
    del X3,Y3,id_ulm_temp,ulm_list_temp
    print 'Get X3,Y3 over.'


    # -------------------------------- 提取线下测试数据集 X4,Y4 --------------------------------------------
    X4,Y4,id_ulm_temp,ulm_list_temp = GetOffDataSet(time_7_15,time_9_15,time_8_15)
    X0.extend(X4)
    Y0.extend(Y4)
    del X4,Y4,id_ulm_temp,ulm_list_temp
    print 'Get X4,Y4 over.'
    
    # -------------------------------- 提取线下测试数据集 X5,Y5 --------------------------------------------
    X5,Y5,id_ulm_temp,ulm_list_temp = GetOffDataSet(time_8_15,time_10_15,time_9_15)
    X0.extend(X5)
    Y0.extend(Y5)
    del X5,Y5,id_ulm_temp,ulm_list_temp
    print 'Get X5,Y5 over.'
    
    # -------------------------------- 提取线下测试数据集 X6,Y6 --------------------------------------------
    X6,Y6,id_ulm_temp,ulm_list_temp = GetOffDataSet(time_9_15,time_11_15,time_10_15)
    X0.extend(X6)
    Y0.extend(Y6)
    del X6,Y6,id_ulm_temp,ulm_list_temp
    print 'Get X6,Y6 over.'

    print 'Get Off DataSet over.'

    # ---------------------------------- 保存线下数据 ---------------------------------------------------------
    df = pd.DataFrame(X0)
    df.to_csv('dataset_csv/X_train_off_0518_sim.csv',encoding='utf-8')

    df = pd.DataFrame(Y0)
    df.to_csv('dataset_csv/Y_train_off_0518_sim.csv',encoding='utf-8')

    del X0,Y0

    # ----------------------------------- 读取、保存线上数据 ------------------------------------------------------
    X_Online,id_ulm_Online = GetOnlineDataSet(time_11_1,time_12_1)
    
    df = pd.DataFrame(X_Online)
    df.to_csv('dataset_csv/X_test_online_0518_sim.csv',encoding='utf-8')
    del X_Online
    
    df = pd.DataFrame(id_ulm_Online)
    df.to_csv('dataset_csv/id_ulm_Online_0518_sim.csv',encoding='utf-8',float_format='string')
    del id_ulm_Online

    print 'Total time: ', time.time() - time_begin

