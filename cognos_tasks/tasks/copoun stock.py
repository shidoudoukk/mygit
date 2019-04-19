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
sql_coupon = """SELECT userid, 
SUM(CASE WHEN code_type BETWEEN 1 AND 9 THEN codenum ELSE 0 END) - 
SUM(CASE WHEN code_type BETWEEN 10 AND 15 THEN codenum ELSE 0 END) AS 旧券余额,
MAX(createtime) 最近一次旧券变动时间 
FROM yooli_usercode_detail
WHERE userid NOT IN
(SELECT userid FROM yooli_usercode_detail
WHERE code_type=15
GROUP BY userid)
GROUP BY userid
HAVING 旧券余额>0
ORDER BY 旧券余额 DESC"""

df_coupon = pd.read_sql(sql_coupon, sqlCONN_2.GetCONN())

print('当前旧券未升级的用户数为%s'%len(df_coupon))

"""有优惠券用户的人均年化投资金额、平均投资期限"""
sql_invest = """SELECT fpi.investUserId,SUM(fpi.investAmount) AS 当前定存宝持有金额
FROM finance_plan_investor fpi
LEFT JOIN finance_plan fp ON fpi.financePlanId=fp.financePlanId
WHERE fpi.`status`= 200 AND TO_DAYS(joinTime) BETWEEN TO_DAYS('2019-01-25') AND TO_DAYS('2019-04-15')
AND fpi.investUserId IN (%s)
GROUP BY fpi.investUserId
HAVING 当前定存宝持有金额>=10000
"""%(','.join(str(i) for i in df_coupon['userid'].values))


df_investcou = pd.read_sql(sql_invest, sqlCONN_1.GetCONN())

df_merge = pd.merge(df_investcou,df_coupon,
                    how='left',
                    left_on='investUserId',
                    right_on='userid',
                    suffixes=('_SQL1','_SQL2'), indicator =True).drop(columns=['_merge'])


file_name = '未进行旧券升级用户存量满1w用户明细.xlsx'
df_merge.to_excel(outfile+file_name, index=False)
print('运行结束...')
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
