#!usr/bin/env python
#-*- coding: utf-8 -*-
__author__ = 'wangsuixue'

import pandas as pd

#####################################################################################################################
#   函数功能：读取 CSV 文件
#
#   输入参数：无
#
#   输出参数：无
#
#   编写时间：2016年5月
#
#####################################################################################################################
def ReadCSV(filePath,type):

    reader = pd.read_csv(filePath,header = None,iterator = True,dtype=type)
    loop = True
    chunckSize = 2000000
    chuncks = []

    k = 0
    while loop:
        try:
            chunck = reader.get_chunk(chunckSize)
            chuncks.append(chunck)

            k = k + chunckSize
            print 'K = ',k
        except StopIteration:
            loop = False
            print 'Iteration is stoped.'

    df = pd.concat(chuncks,ignore_index=True)

    values = df.values
    values = values[1:,1:]

    return values
