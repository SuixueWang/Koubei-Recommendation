#!usr/bin/env python
#-*- coding: utf-8 -*-
__author__ = 'wangsuixue'

import time
import numpy as np
import pandas as pd
import sys
sys.path.append('F:/MachineLearning/StoreRec/source/Koubei_V3/pd_csv_get_chunk')
from ReadCSV import *
import MySQLdb
from ExtNegY import *
from sklearn.metrics import confusion_matrix
import pickle
from sklearn.ensemble import GradientBoostingClassifier


#####################################################################################################################
#   函数功能：预测并根据置信度排序
#
#   输入参数：Save , modelname 
#
#   输出参数：无
#
#   编写时间：2016年5月
#
#############################################################################################################
def model_train_csv(Save = False, modelname = None):

    # 读取 X_train
    Path_1 = 'dataset_csv/X_train_offline_10.csv'
    X = ReadCSV(Path_1,'float64')

    Path_2 = 'dataset_csv/Y_train_offline_11.csv'
    Y = ReadCSV(Path_2,'float64')

    print 'The size of training dataset: ',np.shape(X)

    print 'start training...'
    model = GradientBoostingClassifier(n_estimators=300,subsample=0.7)

    model.fit(X,Y.ravel())

    if Save:
        file = open(modelname,'w')
        pickle.dump(model,file)
        file.close()


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


'''
函数功能： 根据 budget 调整预测结果
'''
################################### 根据 budget 调整预测结果 ##############################################
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