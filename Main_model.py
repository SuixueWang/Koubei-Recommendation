#!usr/bin/env python
#-*- coding: utf-8 -*-
__author__ = 'wangsuixue'
# import mymodel
import time
# import xgboost as xgb
import mymodel_csv
import sys
sys.path.append('F:/MachineLearning/StoreRec/source/project_koubei_v2/pd_csv_get_chunk')
from ReadCSV import *
import pickle

if __name__ == '__main__' :

    time_begin = time.time()
    print 'Starting...'

    # bst = mymodel.model_train(Save = True, modelname = 'model/xgb_800_0.08_0.5_518_150W.model')

    # bst = xgb.Booster({'nthread':1})
    # bst.load_model("model/xgb_800_0.08_0.5_518_150W.model")
    #
    # probas = mymodel.model_predict_csv(bst)
    #
    # select_len = 150 * (10**4)
    # resultname = 'results/xgb_800_0.08_0.5_518_2030_150W_budget.csv'
    # preds_results,merchant_budget = mymodel.model_budget(probas,select_len,resultname)
    #
    # file = open('merchant_budget_800.pkl','w')
    # pickle.dump(merchant_budget,file)
    # file.close()

    # ----------------------- gbdt模型，csv文件操作 -------------------------------------

    mymodel_csv.model_train_csv(Save=True,modelname='model_gbdt_n200_v0.1_sub0.5.pkl')

    print 'Total time: ', time.time() - time_begin







