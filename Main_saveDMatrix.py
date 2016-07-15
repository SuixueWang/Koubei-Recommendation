#!usr/bin/env python
#-*- coding: utf-8 -*-
__author__ = 'wangsuixue'

'''
程序功能: 读取CSV提取的数据集,保存为XGBOOST使用的数据格式
'''

import sys
sys.path.append('F:/MachineLearning/StoreRec/source/Koubei_V3/pd_csv_get_chunk')
from ReadCSV import *
import numpy as np
import xgboost as xgb
from ExtNegY import *


'''
函数功能: 读取CSV格式, 保存为XGBOOST使用的数据格式
'''
def SaveDMatrix():

    # 挑选一些特征
    feature_select = [0,1,4,10,11,13,15,16,\
                      17,18,19,21,22,23,\
                      24,25,26,27,28,29,30,\
                      31,32,33,34,35,36,37,38]

    # 读取 X_train
    Path_1 = 'dataset_csv/X_train_off_0518_sim.csv'
    X_train_offline = ReadCSV(Path_1,'float64')

    X_train_offline = X_train_offline[:,feature_select]

    # 读取 Y_trian
    Path_2 = 'dataset_csv/Y_train_off_0518_sim.csv'
    Y_train_offline = ReadCSV(Path_2,'float64')

    print 'The size of X_train_offline is',np.shape(X_train_offline)
    print 'The size of Y_train_offline is',np.shape(Y_train_offline)

    # 降采样
    X_train_offline,Y_train_offline = ExtNegY(X_train_offline,Y_train_offline,8)

    print 'The size of X_train_offline after extracting is',np.shape(X_train_offline)
    print 'The size of Y_train_offline after extracting is',np.shape(Y_train_offline)

    # 保存 DMatrix 格式
    dtrain = xgb.DMatrix(X_train_offline,label=Y_train_offline)
    dtrain.save_binary("dataset_dmatrix/offline_0518_sim.train.buffer")

    # 回收内存
    del dtrain,X_train_offline,Y_train_offline

    # 读取 X_test_online
    Path_3 = 'dataset_csv/X_test_online_0518_sim.csv'
    X_test_online = ReadCSV(Path_3,'float64')
    
    X_test_online = X_test_online[:,feature_select]
    
    # 分六部分保存
    dtest_part1 = xgb.DMatrix(X_test_online[:3000000,:])
    dtest_part1.save_binary("dataset_dmatrix/online_0518_sim_part1.test.buffer")
    
    dtest_part2 = xgb.DMatrix(X_test_online[3000000:6000000,:])
    dtest_part2.save_binary("dataset_dmatrix/online_0518_sim_part2.test.buffer")
    
    dtest_part3 = xgb.DMatrix(X_test_online[6000000:9000000,:])
    dtest_part3.save_binary("dataset_dmatrix/online_0518_sim_part3.test.buffer")
    
    dtest_part4 = xgb.DMatrix(X_test_online[9000000:12000000,:])
    dtest_part4.save_binary("dataset_dmatrix/online_0518_sim_part4.test.buffer")
    
    dtest_part5 = xgb.DMatrix(X_test_online[12000000:15000000,:])
    dtest_part5.save_binary("dataset_dmatrix/online_0518_sim_part5.test.buffer")
    
    dtest_part6 = xgb.DMatrix(X_test_online[15000000:,:])
    dtest_part6.save_binary("dataset_dmatrix/online_0518_sim_part6.test.buffer")
    
    del dtest_part1,dtest_part2,dtest_part3,dtest_part4,dtest_part5,dtest_part6,  X_test_online


'''
主函数
'''
if __name__ == '__main__':
    print 'save dmatrix start...'

    SaveDMatrix()

    print 'save dmatrix over.'