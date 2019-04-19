# -*- coding:utf-8 -*-

"""
作者：石佳音
用途：查询数据库，获取圈子发帖内容，提取关键词，判断相似帖子并聚类

"""

import pymysql
import jieba
import jieba.analyse
import numpy as np
import pandas as pd
from gensim.models.word2vec import Word2Vec
import  codecs
from sklearn .feature_extraction.text import TfidfTransformer
from sklearn .feature_extraction.text import CountVectorizer
from sklearn.cluster import KMeans

import common.ConnectSql as consql

pd.set_option('max_colwidth',500)

def query():
    #连接mysql
    conn = consql.connectsql()

    sql = """SELECT m.`name` as m_name,a.title,a.create_time,a.content,u.`name` as u_name FROM communitydb.mos_community_article a
JOIN glazedb.da_member m ON a.author_id=m.id
JOIN glazedb.da_member_account dma ON m.id=dma.member_id
JOIN glazedb.sys_user u ON dma.user_id=u.id
WHERE TO_DAYS(a.create_time) >=TO_DAYS('2018-07-01')
AND m.property=0
ORDER BY m.`name`,a.create_time"""
    #读取sql数据形成dataframe
    dataframe = pd.read_sql(sql,conn)
    return dataframe

#读取Excel数据，做测试用
def excel():
    dataframe = pd.read_excel('data.xlsx')
    return dataframe

#分词
def participle_word():
    #dataframe = query()
    dataframe = excel()
    u_name = dataframe['u_name']
    content = dataframe['content']

    dataframe['words'] = dataframe['content'].apply(lambda x: (jieba.cut(x.strip(), cut_all=False, HMM=True)))
    stopwords = [line.strip() for line in open('stopwords.txt','r').readlines()]
    dataframe['seg'] = ''
    d_index = list(dataframe.columns).index('seg')

    for index, rows in dataframe.iterrows():
        for word in rows['words']:
            if word not in stopwords:
                if word != '\t' and word != '\n':
                    rows['seg'] += word + ' '
                    dataframe.iloc[index,d_index] = rows['seg']

    #建立语料库
    corpus = dataframe['seg']
    #建立tfidf模型
    vectorizer = CountVectorizer() #将文本中的词语转换为词频矩阵
    transformer = TfidfTransformer() # 统计每个词语的tf-idf权值
    tfidf = transformer.fit_transform(vectorizer.fit_transform(corpus)) #分别计算tfidf，将文本转化为词频矩阵
    weight = tfidf.toarray()
    words = vectorizer.get_feature_names() #获取词袋模型中的所有词
    title = list(dataframe['title'])

    #KMeans算法
    mykmeans = KMeans(n_clusters = 10)
    y = mykmeans.fit_predict(weight)

    dataframe['lable'] = ''
    #输出各类
    for i in range(0,10):
        lable_i = []
        for j in range(0,len(y)):
            if y[j] == i:
                lable_i.append(dataframe['title'][j])
                dataframe['lable'][j] = i
        print('lable_'+str(i)+str(lable_i))

    #保存为Excel文件
    dataframe.to_excel("result.xlsx")
def main():
    pass

if __name__ == '__main__':
    participle_word()