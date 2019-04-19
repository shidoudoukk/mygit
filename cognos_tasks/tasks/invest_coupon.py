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

curdate = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
print('昨天时间%s'%curdate)

#获取有优惠券的userid
sql_coupon = """SELECT s.userid
FROM yooli_coupons_details s
where s.code_type<20 AND TO_DAYS(s.createtime) >= TO_DAYS('2019-01-25')
GROUP BY s.userid"""

df_coupon = pd.read_sql(sql_coupon, sqlCONN_2.GetCONN())

print('当前有优惠券的用户数为%s'%len(df_coupon))

"""有优惠券用户的人均年化投资金额、平均投资期限"""
sql_invest = """SELECT DATE_FORMAT(fpi.joinTime,'%%Y-%%m-%%d') AS 日期, 
SUM(fpi.investAmount*fp.lockedPeriod/12)/COUNT(DISTINCT fpi.investUserId) as 有优惠券人均年化投资金额,
SUM(fp.lockedPeriod)/COUNT(fpi.id) AS 有优惠券平均投资期限
FROM finance_plan_investor fpi
LEFT JOIN finance_plan fp ON fpi.financePlanId=fp.financePlanId
WHERE TO_DAYS(fpi.joinTime) BETWEEN TO_DAYS('2019-01-25') AND TO_DAYS('%s')
AND fpi.investUserId IN (%s)
GROUP BY DATE_FORMAT(fpi.joinTime,'%%Y-%%m-%%d') 
"""%(curdate, (','.join(str(i) for i in df_coupon['userid'].values)))
print(sql_invest)

df_investcou = pd.read_sql(sql_invest, sqlCONN_1.GetCONN())



"""无优惠券用户的人均年化投资金额、平均投资期限"""
sql_investNan = """SELECT DATE_FORMAT(fpi.joinTime,'%%Y-%%m-%%d') AS 日期, 
SUM(fpi.investAmount*fp.lockedPeriod/12)/COUNT(DISTINCT fpi.investUserId) as 无优惠券人均年化投资金额,
SUM(fp.lockedPeriod)/COUNT(fpi.id) AS 无优惠券平均投资期限
FROM finance_plan_investor fpi
LEFT JOIN finance_plan fp ON fpi.financePlanId=fp.financePlanId
WHERE TO_DAYS(fpi.joinTime) BETWEEN TO_DAYS('2019-01-25') AND TO_DAYS('%s')
AND fpi.investUserId NOT IN (%s)
GROUP BY DATE_FORMAT(fpi.joinTime,'%%Y-%%m-%%d') 
"""%(curdate, (','.join(str(i) for i in df_coupon['userid'].values)))

df_investNanCou = pd.read_sql(sql_investNan, sqlCONN_1.GetCONN())

df_merge = pd.merge(df_investcou,df_investNanCou,
                    how='left',
                    left_on='日期',
                    right_on='日期',
                    suffixes=('_SQL1','_SQL2'), indicator =True).drop(columns=['_merge'])


file_name = '优惠券投资数据.xlsx'
# df_merge.to_excel(outfile+file_name, index=False)
# print('运行结束...')
sqlCONN_1.CloseConn()
sqlCONN_2.CloseConn()



##writer = pd.ExcelFile(outfile+file_name)
# df1 = pd.DataFrame()
# df2 = pd.DataFrame()
#
# df1.to_excel(writer, sheet_name='df_1')
# df2.to_excel(writer, sheet_name='df_2')
#
# writer.save()
