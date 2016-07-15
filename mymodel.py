#!usr/bin/env python
#-*- coding: utf-8 -*-
__author__ = 'wangsuixue'

import time
import xgboost as xgb
import numpy as np
import pandas as pd
import sys
sys.path.append('F:/MachineLearning/StoreRec/source/project_koubei_v2/pd_csv_get_chunk')
from ReadCSV import *
import MySQLdb
from sklearn.metrics import confusion_matrix
import pickle

# 自定义评价函数，f1_score
def evalerror(preds, dtrain):

    labels = dtrain.get_label()
    predict = (preds > 0.5)

    mat = confusion_matrix(labels,predict)
    # print mat

    P = 1.0 * mat[1,1]/(mat[0,1] + mat[1,1])
    R = 1.0 * mat[1,1]/(mat[1,0] + mat[1,1])
    F1_Score = 2*P*R/(P+R)
    print 'F1_Score: ',F1_Score

    return 'f1score',F1_Score

#####################################################################################################################
#   函数功能：从 DMatrix 文件读取数据用于训练
#
#   输入参数：Save, modelname
#
#   输出参数：model
#
#   编写时间：2016年5月
#
#####################################################################################################################
def model_train(Save = False, modelname = None):

    time_load_dtrain = time.time()
    dtrain = xgb.DMatrix('dataset_dmatrix/offline_0518_sim.train.buffer')
    print 'The time of loading dtrain: ',time.time() - time_load_dtrain

    # 设置参数
    param = {'bst:max_depth':5, 'bst:eta':0.08, 'silent':1, 'objective':'binary:logistic'}
    param['nthread'] = 10
    param['subsample'] = 0.5
    # param['eval_metric'] = 'auc'
    # watchlist  = [(dtrain,'eval'), (dtrain,'train')]

    num_round = 800

    # 训练
    print 'Start training.'
    time_train = time.time()
    model = xgb.train(param,dtrain,num_round,feval=evalerror,maximize=True)
    # model = xgb.train(param,dtrain,num_round)
    print 'Training time: ',time.time() - time_train

    # 保存模型
    if Save == True:
        model.save_model(modelname)

    return model


#####################################################################################################################
#   函数功能：从 DMatrix 文件读取数据用于测试和排序
#
#   输入参数：bst
#
#   输出参数：proba_positive
#
#   编写时间：2016年5月
#
#####################################################################################################################
def model_predict_dmatrix(bst):
    print 'Start PredictAndSort...'

    # 读取 X_test_online
    probas = []

    print 'part1'
    dtest_part1 = xgb.DMatrix('dataset_dmatrix/online_0518_sim_part1.test.buffer')
    proba_1 = bst.predict(dtest_part1)
    probas.extend(proba_1)

    print 'part2'
    dtest_part2 = xgb.DMatrix('dataset_dmatrix/online_0518_sim_part2.test.buffer')
    proba_2 = bst.predict(dtest_part2)
    probas.extend(proba_2)

    print 'part3'
    dtest_part3 = xgb.DMatrix('dataset_dmatrix/online_0518_sim_part3.test.buffer')
    proba_3 = bst.predict(dtest_part3)
    probas.extend(proba_3)

    print 'part4'
    dtest_part4 = xgb.DMatrix('dataset_dmatrix/online_0518_sim_part4.test.buffer')
    proba_4 = bst.predict(dtest_part4)
    probas.extend(proba_4)

    print 'part5'
    dtest_part5 = xgb.DMatrix('dataset_dmatrix/online_0518_sim_part5.test.buffer')
    proba_5 = bst.predict(dtest_part5)
    probas.extend(proba_5)

    print 'part6'
    dtest_part6 = xgb.DMatrix('dataset_dmatrix/online_0518_sim_part6.test.buffer')
    proba_6 = bst.predict(dtest_part6)
    probas.extend(proba_6)

    print np.shape(probas)

    del dtest_part1,dtest_part2,dtest_part3,dtest_part4,dtest_part5,dtest_part6
    del proba_1,proba_2,proba_3,proba_4,proba_5,proba_6

    # sort
    proba_positive = []
    for i in range(len(probas)):
        proba_positive.append(probas[i])

    del probas

    # 保存预测完的结果
    # df = pd.DataFrame(proba_positive)
    # df.to_csv('proba_positive_xgb_700.csv',encoding='utf-8')

    return proba_positive


#####################################################################################################################
#   函数功能：保存线上预测结果到CSV文件
#
#   输入参数：predicted,filename
#
#   输出参数：无
#
#   编写时间：2016年5月
#
#####################################################################################################################
def model_saveresults(predicted,filename):
    print 'Start saving results...'

    # 连接数据库
    conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',port=3306,db='db_koubei')
    cur = conn.cursor()

    sql_ul_test = "select user_id,location_id from koubei_test;"

    execute = cur.execute(sql_ul_test)
    results_ul_test = cur.fetchmany(execute)

    wf = open(filename,'w')
    for id_ul_test in results_ul_test:
        id_ul = (id_ul_test[0],id_ul_test[1])
        if predicted.has_key(id_ul):
            merchant_id_list = ":".join(map(str,predicted[id_ul]))
            wf.write('%s,%s,%s\n'%(id_ul[0],id_ul[1],merchant_id_list))

    wf.close()

    # 关闭数据库
    conn.commit()
    cur.close()
    conn.close()





#####################################################################################################################
#   函数功能：预测并根据置信度排序
#
#   输入参数：bst
#
#   输出参数：proba_positive
#
#   编写时间：2016年5月
#
#####################################################################################################################
def model_predict_csv(bst):
    print 'Start PredictAndSort...'

    # 读取 X_test_online
    filePath = 'dataset_csv/X_test_online_0516_sim.csv'
    reader = pd.read_csv(filePath,header = None,iterator = True,dtype = 'float64')
    loop = True
    chunckSize = 3000000
    probas = []

    reader.get_chunk(1) # 去掉第一行标题
    k = 0
    time_predict = time.time()
    while loop:
        try:
            chunck = reader.get_chunk(chunckSize)
            X_test = chunck.values
            X_test = X_test[:,1:]
            dtest = xgb.DMatrix(X_test)

            # 分块预测
            time_predict = time.time()
            proba = bst.predict(dtest)
            print 'Predicting time: ',time.time() - time_predict
            probas.extend(proba)

            k = k + 1
            print 'K = ',k

        except StopIteration:
            loop = False
            print 'Iteration is stoped.'
    print 'Time predict is ',time.time() - time_predict
    print np.shape(probas)
    del chunck,X_test,proba

    # sort
    proba_positive = []
    for i in range(len(probas)):
        proba_positive.append(probas[i])

    del probas

    # 保存预测完的结果
    # df = pd.DataFrame(proba_positive)
    # df.to_csv('proba_positive_xgb_700.csv',encoding='utf-8')

    return proba_positive



################################### 根据 budget 调整预测结果 ##############################################
'''
函数功能： 根据 budget 调整预测结果
'''
def model_budget(proba_positive,select_len,resultname):
    # ---------------------------------- 读取 id_ulm_Online --------------------------------------------
    filePath = 'dataset_csv/id_ulm_Online_0516.csv'
    id_ulm = ReadCSV(filePath,object)
    print 'Load id_ulm_Online over.'

    # ------------------------------------  排序 --------------------------------------------
    print 'Sort start.'
    preds = zip(id_ulm,proba_positive)
    preds_sort = sorted(preds,key=lambda x:x[1], reverse = True)

    # ---------------------------------- 读取 budget ----------------------------------------
    # 连接数据库
    conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',port=3306,db='db_koubei')
    cur = conn.cursor()

    print 'read budget info'
    sql_budget = "select merchant_id,budget from merchant_info;"

    execute = cur.execute(sql_budget)
    results_budget = cur.fetchmany(execute)

    # 把约束条件放字典
    merchant_budget = {}
    for bid in results_budget:
        merchant_budget[bid[0]] = [ int(bid[1]), 0, 0.0 ]

    # -------------------------------------- 取出 test 表结果 ---------------------------------------
    sql_ul_test = "select user_id,location_id from koubei_test;"

    execute = cur.execute(sql_ul_test)
    results_ul_test = cur.fetchmany(execute)

    ul_test_dict = {}
    for ulid in results_ul_test:
        ul_id = (ulid[0],ulid[1])
        ul_test_dict[ul_id] = 1


    # --------------------------------------- 筛选结果 --------------------------------------------
    preds_results = {}
    print 'select_len = ',select_len
    for i in range(select_len):
        item  = preds_sort[i]
        id_ul = (item[0][0],item[0][1])
        if ul_test_dict.has_key(id_ul):
            id_m  = item[0][2]
            if merchant_budget[id_m][1] < merchant_budget[id_m][0]:
                merchant_budget[id_m][1] += 1
                merchant_budget[id_m][2] = 1.0 * merchant_budget[id_m][1] / merchant_budget[id_m][0]
                if preds_results.has_key(id_ul):
                    if len(preds_results[id_ul]) < 10:
                        preds_results[id_ul].append(id_m)
                    else:
                        print 'larger than 10'
                else:
                    preds_results.setdefault(id_ul,[]).append(id_m)
        else:
            print 'ul not in ul_test'


    wf = open(resultname,'w')
    for id_ul_test in preds_results:
        merchant_id_list = ":".join(map(str,preds_results[id_ul_test]))
        wf.write('%s,%s,%s\n'%(id_ul_test[0],id_ul_test[1],merchant_id_list))

    wf.close()

    conn.commit()
    cur.close()
    conn.close()

    return preds_results,merchant_budget