# -*- coding: utf-8 -*-
'''
Created on 2018-10-23

@author: NGL
'''

import pymysql
import pymongo
import datetime
import os
import configparser
from impala.dbapi import connect
from impala.util import as_pandas

#config = configparser.ConfigParser()

bWin = True
linuxPath =  '/ssd/script/config/timing_config/config.cnf'
winPath = 'E:\\tables_out\\config.ini'
#winPath = '..\\config.ini'
excelPath = 'E:\\tables_out\\'

if bWin:
    sConfigPath = winPath
else:
    sConfigPath = linuxPath
class clMySql:
  
    __file=''
    __conn=None
    __cur=None  
    __host = ''
    __user = ""
    __pw = ""
    __db = "fuscent"
    __port = 23307
    __charset = 'utf8'
    __bShow = False
        
    def __init__(self, path, configKey, bShowInfo=False): #  Constructor
             
        self.__bShow = bShowInfo
        self.__file = path
        if bShowInfo:
            self.__ShowInfo()

        config = configparser.ConfigParser()
        print(path) 
        config.read(path.strip(),encoding='utf-8-sig') # "utf-8-sig" if ANSI(utf8)
        print(config.sections())

        self.__host = config[configKey]['host'].replace("\n", '').strip()
        self.__user = config[configKey]['user'].replace("\n", '').strip()
        self.__pw = config[configKey]['pw'].replace("\n", '').strip()

        if config.has_option(configKey,'db'):
            self.__db = config[configKey]['db'].replace("\n", '').strip()
        if config.has_option(configKey,'port'):
            self.__port = int(config[configKey]['port'].replace("\n", '').strip())
        if config.has_option(configKey,'charset'):
            self.__charset = config[configKey]['charset'].replace("\n", '').strip()
        if self.__bShow:
            print("connect server: %s with account %s (%s) on db: %s at port: %s with charset %s"  
                  % (self.__host,self.__user,self.__pw,self.__db,self.__port,self.__charset))
        
        self.__GetConnect()
                       
    def __SetConfig(self,path):
        self.__file = path
                        
    def __GetConnect(self):
        try:
            self.__conn=pymysql.connect(self.__host,self.__user,self.__pw,self.__db,self.__port,charset=self.__charset)
            self.__cur = self.__conn.cursor()
            print('%s with %s connect successfully--'%(self.__host,self.__user))

        except Exception as e:
            print("!!!database connect error---%s"%e)
    def GetCONN(self):
        return self.__conn
    
    def __ShowInfo(self):
        print("---server: %s at port=%d" % (self.__host,self.__port))
        print("---with log name: %s at password: %s" % (self.__user,self.__pw)) 
        print("---in database: %s at the charset %s" %(self.__db,self.__charset))
        
    def GetExecute(self,queryStement):
        try:
            if self.__cur:
                self.__cur.execute(queryStement)
                return self.__cur.fetchall()
            else:
                return None
        
        except Exception as e:
            print('!!!cur error %s---' % e)
            return None

    def CallProc(self,procName,arc):
        resultSet=None
        try:
            if self.__cur:
               resultSet=self.__cur.callproc(procName,arc)
               #resultSet = self.__cur.stored_results()
        except Exception as e:
            self.__conn.rollback()  # 事务回滚
            print('write failed, rollback.', e)
            return None
        else:
            self.__conn.commit()  # 事务提交
      #      print("call proc committed!")
            return resultSet


    def DoWrite(self,writeSql,sValues=None):
        try:
            if self.__cur:
                self.__cur.execute(writeSql,sValues)
            else:
                print("cur error, no actions.")

        except Exception as e:
            self.__conn.rollback()  # 事务回滚
            print('write failed, rollback.', e)
        else:
            self.__conn.commit()  # 事务提交
            #print('commit successfully.', self.__cur.rowcount)
        
    def CloseConn(self):
        try:
            if self.__cur:
                self.__cur.close()
            if self.__conn:
                self.__conn.close()
        except Exception as e:  
            print("close sql connection error!----%s"%e)



# define mongo class
class clMongo():
    # members
    __cur=None
    __dbConn=None
    __dbName=''
    __currentDbName=''
    __Clinent=None
    __collectionName='cases'
    __host='192.168.10.121'
    __user = ""
    __pw = ""
    __port = 8888
    __charset = 'utf8'
        
    def __init__(self, path, configKey, bShowInfo=False):

        config = configparser.ConfigParser()
        print("---config path is: %s" % path) 
        config.read(path.strip(),encoding='utf-8-sig')

        self.__host = config[configKey]['host'].replace("\n", '').strip()
        self.__user = config[configKey]['user'].replace("\n", '').strip()
        self.__pw = config[configKey]['pw'].replace("\n", '').strip()

        if config.has_option(configKey,'switchdb'):
            self.__currentDbName = config[configKey]['switchdb'].replace("\n", '').strip()
        if config.has_option(configKey,'db'):
            self.__dbName = config[configKey]['db'].replace("\n", '').strip()
        if config.has_option(configKey,'collection'):
            self.__collectionName = config[configKey]['collection'].replace("\n", '').strip()
        if config.has_option(configKey,'port'):
            self.__port = int(config[configKey]['port'].replace("\n", '').strip())
        if config.has_option(configKey,'charset'):
            self.__charset = config[configKey]['charset'].replace("\n", '').strip()
        
        if bShowInfo:
            self.__ShowInfo()
            print("..... try to connect server: %s with account %s (%s) on db: %s(%s) at port: %s with charset %s ...."  
                  % (self.__host,self.__user,self.__pw,self.__dbName,self.__collectionName,self.__port,self.__charset))
  
        self.__GetConnect()
                       
                        
    def __GetConnect(self):
        try:            
            #connect mongo server                                   
            self.__Clinent = pymongo.MongoClient(self.__host,self.__port)                
            #connect database
            self.__dbConn = self.__Clinent.get_database(self.__dbName) 
         
            
            #check the database authenticate              
            if self.__dbConn.authenticate(self.__user,self.__pw):
                print('---connected "%s" with "%s" --'%(self.__host,self.__user))  
                if self.__currentDbName == '':
                    pass
                else:
                    if self.__currentDbName != self.__dbName:
                    #do switch db
                        print('---swith to db: %s---' % self.__currentDbName)
                        self.__dbConn = self.__Clinent.get_database(self.__currentDbName)
            else:
                print('---failed to connect "%s" with "%s" for authenticate--'%(self.__host,self.__user))
                return None
            
            #connect collection
            self.__cur = self.__dbConn.get_collection(self.__collectionName)
            print('...connecting %s with %s at database %s at collection %s--'%(self.__host,self.__user,self.__dbName,self.__collectionName))

            
        except Exception as e:
            print("!!!database connect error---%s"%e)
    
    def __ShowInfo(self):
        print("---server: %s at port=%d" % (self.__host,self.__port))
        print("---with log name: %s at password: %s" % (self.__user,self.__pw)) 
        print("---in database: %s at the collection: %s" %(self.__dbName, self.__collectionName))
    
    def GetCollection(self):
        try:
            if self.__cur:
                return self.__cur
            else:
                return None
        
        except Exception as e:
            print('!!!cur error %s---' % e)
            return None  
        
    def ChangeDb(self,name):
        try:
            return self.__Clinent[name]
        except Exception as e:
            print('!!! no such db: %s---' % e)
            return None   

    def ChangeCollection(self,dbName, colName):
        #print('**************** %s' % self.__Clinent)
        try:
            return self.__Clinent.get_database(dbName)[colName]
        except Exception as e:
            print('!!!collection error : %s---' % e)
            return None    
        
    def CloseConn(self):
        try:
            if self.__Clinent:
                return self.__Clinent.close()
        except Exception as e:  
            print("close mongo connection error!----%s"%e)


class clHive():
    __file = ''
    __conn = None
    __cur = None
    __host = ""
    __user = ""
    __pw = ""
    __db = "zhugeio"
    __port = 21050
    #__charset = 'utf8'
    __bShow = False

    def __init__(self, path, configKey, bShowInfo=False):  # Constructor

        self.__bShow = bShowInfo
        self.__file = path
        if bShowInfo:
            self.__ShowInfo()

        config = configparser.ConfigParser()
        print(path)
        config.read(path.strip(), encoding='utf-8-sig')  # "utf-8-sig" if ANSI(utf8)
        print(config.sections())

        self.__host = config[configKey]['host'].replace("\n", '').strip()
        self.__user = config[configKey]['user'].replace("\n", '').strip()
        self.__pw = config[configKey]['pw'].replace("\n", '').strip()

        if config.has_option(configKey, 'db'):
            self.__db = config[configKey]['db'].replace("\n", '').strip()
        if config.has_option(configKey, 'port'):
            self.__port = int(config[configKey]['port'].replace("\n", '').strip())
        # if config.has_option(configKey, 'charset'):
        #     self.__charset = config[configKey]['charset'].replace("\n", '').strip()
        if self.__bShow:
            print("connect server: %s with account %s (%s) on db: %s at port: %s " # with charset %s
                  % (self.__host, self.__user, self.__pw, self.__db, self.__port)) #, self.__charset

        self.__GetConnect()

    def __SetConfig(self, path):
        self.__file = path

    def __GetConnect(self):
        try:
            self.__conn = connect(host=self.__host, port=self.__port, database=self.__db, ) #charset=self.__charset
            self.__cur = self.__conn.cursor()
            print('%s with %s connect successfully--' % (self.__host, self.__user))

        except Exception as e:
            print("!!!database connect error---%s" % e)

    def GetCONN(self):
        return self.__conn
        #return self.__cur

    def __ShowInfo(self):
        print("---server: %s at port=%d" % (self.__host, self.__port))
        print("---with log name: %s at password: %s" % (self.__user, self.__pw))
        print("---in database: %s at the charset %s" % (self.__db, self.__charset))

    def GetExecute(self, queryStement):
        try:
            if self.__cur:
                self.__cur.execute(queryStement)
                return self.__cur
                #return self.__cur.fetchall()
            else:
                return None

        except Exception as e:
            print('!!!cur error %s---' % e)
            return None

    def CallProc(self, procName, arc):
        resultSet = None
        try:
            if self.__cur:
                resultSet = self.__cur.callproc(procName, arc)
                # resultSet = self.__cur.stored_results()
        except Exception as e:
            self.__conn.rollback()  # 事务回滚
            print('write failed, rollback.', e)
            return None
        else:
            self.__conn.commit()  # 事务提交
            #      print("call proc committed!")
            return resultSet

    def DoWrite(self, writeSql, sValues=None):
        try:
            if self.__cur:
                self.__cur.execute(writeSql, sValues)
            else:
                print("cur error, no actions.")

        except Exception as e:
            self.__conn.rollback()  # 事务回滚
            print('write failed, rollback.', e)
        else:
            self.__conn.commit()  # 事务提交
            # print('commit successfully.', self.__cur.rowcount)

    def CloseConn(self):
        try:
            if self.__cur:
                self.__cur.close()
            if self.__conn:
                self.__conn.close()
        except Exception as e:
            print("close sql connection error!----%s" % e)

def main():
    # create the connection to sql and mongo separately 
    sql=clMySql(sConfigPath,'default_glazedb',True)
    mongo = clMongo(sConfigPath,'mongo_bari',True)
    hive = clHive(sConfigPath,'hive_base', True)

if __name__ == '__main__':
    print("...run begin... %s" % datetime.datetime.now())    
    main()
    print("...run finished %s" % datetime.datetime.now())

    
