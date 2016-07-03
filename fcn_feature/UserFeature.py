#!usr/bin/env python
#-*- coding: utf-8 -*-
__author__ = 'wangsuixue'

import time
import numpy as np
import MySQLdb
from scipy.stats.stats import pearsonr

#####################################################################################################################
#   函数功能：计算用户特征
#
#   输入参数：无
#
#   输出参数：无
#
#   编写时间：2016年5月
#
#####################################################################################################################
def CalUserFeature(id_ulm,time_start,time_end_feature):

    # 连接数据库
    conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',port=3306,db='db_koubei')
    cur = conn.cursor()

    id_u = set(list(np.array(id_ulm)[:,0]))
    print 'The number of id_u: ',len(id_u)
    # 用逗号分隔开
    user_list = ",".join(map(str,list(id_u)))

    # ---------------------------- 用户线下特征 UT1 ----------------------------------------------------------
    # 构建数据库语句，查找线下用户的活跃度
    sql_uid = "select user_id,count(*) from koubei_train\
           where user_id in (%s) and\
                 time_stamp >= '%s' and\
                 time_stamp <  '%s' \
            group by user_id;"%(user_list,time_start,time_end_feature)

    execute = cur.execute(sql_uid)
    uid_off = cur.fetchmany(execute)

    user_buy_off = {}
    for uid in uid_off:
        user_buy_off[uid[0]] = uid[1]

    print 'The number of user_buy_off: ',len(uid_off)

    # ------------------------------ 用户线上特征 UT4 -----------------------------------------------------------
    # 构建数据库语句，查找线上用户的购买活跃度
    sql_uid = "select user_id,count(*) from taobao\
           where user_id in (%s) and\
                 time_stamp >= '%s' and\
                 time_stamp <  '%s' and\
                 online_action_id = '1'\
            group by user_id;"%(user_list,time_start,time_end_feature)

    execute = cur.execute(sql_uid)
    uid_online_buy = cur.fetchmany(execute)


    user_online_buy = {}
    for uid in uid_online_buy:
        user_online_buy[uid[0]] = uid[1]

    print 'The number of uid_online_buy: ',len(uid_online_buy)


    # ------------------------------- 用户线上特征 UT5 ----------------------------------------------------------
    # 构建数据库语句，查找线上用户的点击活跃度
    sql_uid = "select user_id,count(*) from taobao\
           where user_id in (%s) and\
                 time_stamp >= '%s' and\
                 time_stamp <  '%s' and\
                 online_action_id = '0'\
            group by user_id;"%(user_list,time_start,time_end_feature)

    execute = cur.execute(sql_uid)
    uid_online_click = cur.fetchmany(execute)


    user_online_click = {}
    for uid in uid_online_click:
        user_online_click[uid[0]] = uid[1]

    print 'The number of uid_online_click: ',len(uid_online_click)

    # -------------------------------------------------- 提取用户特征 --------------------------=---------------------
    # 提取用户特征
    X_user = []
    for id in id_ulm:
        U = id[0]

        UT1  = user_buy_off.get(U,0)
        UT4  = user_online_buy.get(U,0)
        UT5  = user_online_click.get(U,0)

        UT12 = UT4/(UT4 + UT5 + 1.0)         # 点击/购买转换率

        X_user.append((UT1,UT4,UT5,UT12))

    # 关闭数据库
    conn.commit()
    cur.close()
    conn.close()

    return X_user

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