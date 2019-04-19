#!/usr/bin/env python
# coding: utf-8

# In[35]:


# -*- coding:utf-8 -*-
import pandas as pd


# In[36]:


filepath = 'D:/data.xlsx'


# In[37]:


forecastnum = 5


# In[38]:


data = pd.read_excel(filepath, index_col=u'日期')


# In[39]:


import matplotlib.pyplot as plt


# In[40]:


plt.rcParams['font.sans-serif']=['SimHei']


# In[41]:


plt.rcParams['axes.unicode_minus']=False


# In[42]:


data.plot()


# In[43]:


from statsmodels.graphics.tsaplots import plot_acf


# In[44]:


plot_acf(data['度量值']).show()


# In[45]:


from statsmodels.tsa.stattools import adfuller as ADF


# In[46]:


print('原始序列的ADF检验结果为：',ADF(data['度量值']))


# In[47]:


D_data = data.diff().dropna()


# In[48]:


D_data.columns = ['度量值差分']


# In[49]:


D_data.plot()


# In[50]:


plot_acf(D_data).show()


# In[51]:


from statsmodels.graphics.tsaplots import plot_pacf


# In[52]:


plot_pacf(D_data).show()


# In[53]:


print('差分序列的ADF检验结果为，',ADF(D_data['度量值差分']))


# In[54]:


from statsmodels.stats.diagnostic import acorr_ljungbox


# In[55]:


print('差分序列后的白噪声检验结果为，', acorr_ljungbox(D_data['度量值差分'],lags=2))


# In[56]:


from statsmodels.tsa.arima_model import ARIMA


# In[72]:


pmax = int(len(D_data)/10)


# In[73]:


qmax = int(len(D_data)/10)


# In[74]:


bic_matrix = []


# In[75]:


for p in range(pmax+1):
    tmp = []
    for q in range(qmax+1):
        try:
            tmp.append(ARIMA(data, (p,1,q)).fit().bic)
        except:
            tmp.append(None)
    bic_matrix.append(tmp)


# In[76]:


bic_matrix


# In[77]:


bic_matrix = pd.DataFrame(bic_matrix)


# In[79]:


bic_matrix


# In[85]:


bic_matrix.dtypes


# In[88]:


bic_matrix[8:11] = bic_matrix[8:11].astype(float)


# In[94]:


bic_matrix[[8,10,11]]= bic_matrix[[8,10,11]].astype(float)


# In[95]:


bic_matrix[[8,10,11]].dtypes


# In[96]:


bic_matrix.dtypes


# In[99]:


p,q = bic_matrix.stack().idxmin()


# In[100]:


print('BIC最小的p,q值为%s,%s'%(p,q))


# In[101]:


model = ARIMA(data, (p,1,q)).fit()


# In[102]:


model.summary2()


# In[104]:


model.forecast(5)


# In[ ]:




