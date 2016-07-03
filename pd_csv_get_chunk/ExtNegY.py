#!usr/bin/env python
#-*- coding: utf-8 -*-
__author__ = 'wangsuixue'

import time
import copy
import numpy as np
import pickle



###########################################################################################
def ExtNegY(x,y,ratio):
	from sklearn import cross_validation
	x0 = []
	y0 = []
	x1 = []
	y1 = []

	for i in range(len(y)):
		if(y[i] == 0):
			x0.append(x[i,:])
			y0.append(y[i])
		else:
			x1.append(x[i,:])
			y1.append(y[i])
		# x0 = x[y == 0]
		# y0 = y[y == 0]
		# x1 = x[y == 1]
		# y1 = y[y == 1]

	train_size = 1.0 * len(y1) * ratio / len(y0)
	x0, X_test, y0, Y_test = cross_validation.train_test_split(x0,\
                                y0, train_size = train_size, random_state=0)

	x1.extend(x0)
	y1.extend(y0)

	return np.array(x1),np.array(y1)


#####################################################################################################################
#   函数功能：测试
#
#   输入参数：无
#
#   输出参数：无
#
#   编写时间：2016年5月
#
#####################################################################################################################
if __name__ == '__main__':
    
    print 'ExtNegY function.'
