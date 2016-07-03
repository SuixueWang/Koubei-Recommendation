#!usr/bin/env python
#-*- coding: utf-8 -*-
__author__ = 'wangsuixue'

import time
import numpy as np
import MySQLdb
from scipy.stats.stats import pearsonr


#####################################################################################################################
#   函数功能：计算实体店特征
#
#   输入参数：无
#
#   输出参数：无
#
#   编写时间：2016年5月
#
#####################################################################################################################
def MerchantFeature(id_ulm,time_start,time_end_feature):

    # 连接数据库
    conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',port=3306,db='db_koubei')
    cur = conn.cursor()

    id_m = set(list(np.array(id_ulm)[:,2]))
    print 'The number of id_m: ',len(id_m)
    # 用逗号分隔开
    merchant_list = ",".join(map(str,list(id_m)))

    # ------------------------------ 实体店线下特征 MT3 -----------------------------------------------------------
    # 构建数据库语句，查找实体店线下连锁店的个数
    sql_uid = "select merchant_id,count(distinct location_id) from koubei_train\
           where merchant_id in (%s) and\
                 time_stamp >= '%s' and\
                 time_stamp <  '%s' \
            group by merchant_id;"%(merchant_list,time_start,time_end_feature)

    execute = cur.execute(sql_uid)
    mid_off_loc = cur.fetchmany(execute)

    mer_off_loc = {}
    for uid in mid_off_loc:
        mer_off_loc[uid[0]] = uid[1]

    print 'The number of mid_off_loc: ',len(mid_off_loc)



    # -------------------------------------------------- 提取用户特征 --------------------------=---------------------
    # 提取用户特征
    X_merchant = []
    for id in id_ulm:
        M = id[2]
        MT3  = mer_off_loc.get(M,0)

        X_merchant.append(MT3)


    # 关闭数据库
    conn.commit()
    cur.close()
    conn.close()

    return X_merchant

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
    
    print 'Total time: ', time.time() - time_begin