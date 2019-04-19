# -*- coding:utf-8 -*-

import sys
sys.path.append("..")
#from tasks import request
import os
import pandas as pd
import common.dataConnect as db
import datetime
import pandas as np
from xlutils.copy import copy
import xlrd,xlwt
from openpyxl import load_workbook, Workbook
from openpyxl.writer.excel import ExcelWriter
from openpyxl.styles import Border, Alignment, numbers, Side, Font

excel_file = 'D:/普惠金融每日问答活动数据需求0408.xlsx'
outfile = 'D:/'
winPath = 'E:\\Python\\demo\\config.ini'
sConfigPath = winPath

# 连接数据库, sqlCONN_1为fuscent， sqlCONN_2为yl
sqlCONN_1 = db.clMySql(sConfigPath, 'default_glazedb', False)
sqlCONN_2 = db.clMySql(sConfigPath, 'default_activity', False)

curdate = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
print('昨天时间%s'%curdate)

#读取上次的查询数据，做差分
df_hexcel = pd.read_excel(excel_file)
print('上次查询数据条数为%s'%len(df_hexcel))

"""查询截止昨天参与活动的user_id及对应的活动奖金金额、活动参与天数、答题数、答对数、正确率"""
sql_actuser = """SELECT uid, SUM(CASE WHEN type=1 THEN price else 0 END) AS 活动奖金金额,
COUNT(DISTINCT stage) 活动参与天数,
COUNT(id) 答题数,
COUNT(CASE WHEN is_correct=1 THEN id ELSE NULL END) 答对数,
COUNT(CASE WHEN is_correct=1 THEN id ELSE NULL END)/COUNT(id) 正确率
FROM yooli_user_answer  
WHERE TO_DAYS(createtime)<=TO_DAYS('%s')
GROUP BY uid"""%curdate
df_actuser = pd.read_sql(sql_actuser, sqlCONN_2.GetCONN())

"""将用户数据与上次历史数据做差分，得到本次的用户群体"""
df_merge = pd.merge(df_actuser,df_hexcel,how='left',
                    left_on='uid',
                    right_on='uid',
                    suffixes=('_excel','_SQL'), indicator =True)
df_merge = df_merge[df_merge['_merge'] == 'left_only'].drop(columns=['_merge'])
print('本次用户群体数为%s'%len(df_merge))

"""查询用户注册时间及当前账户可用余额"""
sql_member = """SELECT userId as uid, registerTime AS 注册时间, cash as 当前账户可用余额 FROM user_main_v WHERE userId IN (%s)"""\
             % (','.join(str(i) for i in df_merge['uid'].values))
df_regist = pd.read_sql(sql_member, sqlCONN_1.GetCONN())

"""merge得到用户注册时间及当前账户可用余额"""
df_merge = pd.merge(df_merge,df_regist,how='left',
                    left_on='uid',
                    right_on='uid',
                    suffixes=('_SQL1','_SQL2'), indicator =True).drop(columns=['_merge'])
print('新增注册时间及账户可用余额%s'%df_merge.columns)

"""查询定存宝持余额及活动开始后年化出借金额"""
sql_finance = """SELECT fpi.investUserId as uid,SUM(fpi.investAmount) AS 当前定存宝持有金额,
SUM(CASE WHEN fpi.joinTime>='2019-04-01 10:00:00'
AND fp.lockedPeriod!=1  THEN fpi.investAmount*fp.lockedPeriod/12 ELSE 0 END) as 活动开始后年化出借金额 
FROM finance_plan_investor fpi
LEFT JOIN finance_plan fp ON fpi.financePlanId=fp.financePlanId
WHERE fpi.`status`= 200 AND fpi.investUserId IN (%s)
GROUP BY fpi.investUserId
"""% (','.join(str(i) for i in df_merge['uid'].values))
df_finance = pd.read_sql(sql_finance, sqlCONN_1.GetCONN())

"""merge得到当前定存宝余额及年化出借金额"""
df_merge = pd.merge(df_merge,df_finance,how='left',
                    left_on='uid',
                    right_on='uid',
                    suffixes=('_SQL1','_SQL2'), indicator =True).drop(columns=['_merge'])
print('最终merge数据%s，字段列表%s'%(len(df_merge), df_merge.columns.values))

df_merge.fillna(0)
outname = '输出会员'+curdate+'.xlsx'
df_merge.to_excel(outfile+outname,index =False)

sqlCONN_1.CloseConn()
sqlCONN_2.CloseConn()




