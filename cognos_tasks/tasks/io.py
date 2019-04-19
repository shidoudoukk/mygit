# -*- coding:utf-8 -*-

import json
import requests
from requests.auth import HTTPBasicAuth

urla = 'http://119.253.249.143:9003/#/notebook/2C85RT5PB'

r = requests.get(urla, auth=HTTPBasicAuth("17610261231","zhangqianqian")).json()

#改变sqldf的默认驱动
options(sqldf.driver='SQLite')

#链接后台数据库
con <- dbConnect(MySQL(),dbname="fuscent",username="yachao.zhang",
                 password="newpass1",host="119.254.115.72",port=23307)

#dbDisconnect(con)
#连接诸葛数据
#-----------------------------------------------------------------------------------
drv <- odbc::odbc()
impala <- src_impala(
  drv = drv,
  driver = 'Cloudera ODBC Driver for Impala',
  host = '119.253.249.145',
  port = 21050,
  database = 'zhugeio'
  #  uid = 'admin',
  #  pwd = 'admin'
)