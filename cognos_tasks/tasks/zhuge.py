from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
import sys
sys.path.append("..")
import common.dataConnect as db

winPath = 'E:\\Python\\demo\\config.ini'
sConfigPath = winPath
sqlCONN = db.clHive(sConfigPath, 'hive_base', False)
sqlbase = """select
from_unixtime(e.begin_date,'HH') as h,    
count(e.zg_id)/2 as num
from
    b_user_event_all_3  e  
where
    e.platform >  0 
    and e.begin_day_id between 20190413 and 20190414
    and e.event_id = 1649 
group by from_unixtime(e.begin_date,'HH')
order by from_unixtime(e.begin_date,'HH')
"""
df = as_pandas(sqlCONN.GetExecute(sqlbase))
print(df)




# df_actuser = pd.read_sql(sql_actuser, sqlCONN_2.GetCONN())
