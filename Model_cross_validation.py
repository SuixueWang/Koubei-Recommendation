#!usr/bin/env python
#-*- coding: utf-8 -*-

import time
import numpy as np
import xgboost as xgb
from sklearn.metrics import confusion_matrix

# 自定义评价函数，f1_score
def evalerror(preds, dtrain):

    labels = dtrain.get_label()
    predict = (preds > 0.5)

    mat = confusion_matrix(labels,predict)
    # print mat

    P = 1.0 * mat[1,1]/(mat[0,1] + mat[1,1])
    R = 1.0 * mat[1,1]/(mat[1,0] + mat[1,1])
    F1_Score = 2*P*R/(P+R)
    # print 'F1_Score:',F1_Score
    return 'f1score',F1_Score

def logregobj(preds, dtrain):
    labels = dtrain.get_label()
    preds = 1.0 / (1.0 + np.exp(-preds))
    grad = preds - labels
    hess = preds * (1.0-preds)
    return grad, hess



def cross_validation():

    dtrain = xgb.DMatrix('dataset_dmatrix/offline_0516_sim.train.buffer')

    param = {'max_depth':5, 'eta':0.08, 'silent':1, 'objective':'binary:logistic'}
    param['nthread'] = 8
    param['subsample'] = 0.5
    num_round = 1500

    print ('running cross validation')
    xgb.cv(param, dtrain, num_round, nfold=3,
		   show_progress=True,feval=evalerror ,seed = 0,show_stdv=False,maximize=True)


def train_fscore():

    dtrain = xgb.DMatrix('dataset_dmatrix/offline_0516.train.buffer')
    print dtrain.slice(22)

    param = {'max_depth':5, 'eta':0.05, 'silent':1, 'objective':'binary:logistic'}
    param['nthread'] = 1
    param['subsample'] = 0.5
    num_round = 100
    watchlist  = [(dtrain,'eval'), (dtrain,'train')]

    print ('training')
    # bst = xgb.train(param, dtrain, num_round,watchlist,feval=evalerror,maximize=True)


if __name__ == '__main__':
    time1 = time.time()
    # train_fscore()
    cross_validation()
    print 'Total time: ',time.time() - time1







