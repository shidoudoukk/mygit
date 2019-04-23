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

outfile = 'D:/'
winPath = 'E:\\Python\\demo\\config.ini'
sConfigPath = winPath

# 连接数据库, sqlCONN_1为fuscent， sqlCONN_2为yl
sqlCONN_1 = db.clMySql(sConfigPath, 'default_glazedb', False)
sqlCONN_2 = db.clMySql(sConfigPath, 'default_activity', False)

curdate = (datetime.datetime.now()).strftime('%Y-%m-%d')

sql_actuser = """SELECT uid FROM yooli_user_answer
GROUP BY uid"""
df_actuser = pd.read_sql(sql_actuser, sqlCONN_2.GetCONN())

sql_invest = """
SELECT fpi.investUserId,SUM(fpi.investAmount*fp.lockedPeriod/12) 年化
FROM finance_plan_investor fpi
LEFT JOIN finance_plan fp ON fpi.financePlanId=fp.financePlanId
WHERE TO_DAYS(fpi.joinTime) BETWEEN TO_DAYS('2019-04-01') AND TO_DAYS(NOW())
AND fpi.investUserId NOT IN (%s)
GROUP BY fpi.investUserId
HAVING 年化>50000
"""%(','.join(str(i) for i in df_actuser['uid'].values))
df_out = pd.read_sql(sql_invest, sqlCONN_1.GetCONN())

print('活动期间年化出借>5万且未参加活动用户%s'%(len(df_out)))
outname = '短信营销'+curdate+'.xlsx'
df_out.to_excel(outfile+outname,index =False)





