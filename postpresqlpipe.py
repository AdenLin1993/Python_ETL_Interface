#!/usr/bin/python
#-*- coding: utf-8 -*-

"""python3 自带库"""
import os
import re
import sys
import json
import time
import logging
import datetime
import configparser

"""python 第三方库"""
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

"""
#暂时收录两种连接postpresql的方式，根据需求选择使用。

##1、SqlalchemyPipe类是使用sqlalchemy这个包建立一个连接池，适用于建立server、多线程时使用，python中有名的ORM工具。
    目前暂时将这个结合pandas使用,to_sql快速创建表结构。read_sql快速取得数据。(一次性使用的程式、或者只读程式)
    注意：连接的库名不要错了
    postpresql中有一个更加强大的功能，copy_from()方法，有时间可以学习测试一下。

##2、MyPymysql类是使用psycopg2这个包进行单次连接，创建一个实例类即生成一个连接，一个实例类中的程式执行完毕即断开连接，
    测试发现单个实例类中连接时长会根据数据库设置默认连接多长时间无操作会自动断开。
"""

class MySqlalchemyPool(object) :
    """
    fun:使用sqlalchemy创建ORM工具与数据库通讯。
    """
    def __init__(self,settingdict) :
        """
        param:settingdict --> {'user':ieuser,'password':iepwd,'host':iehost,'port':ieport,'database':iedb}
        fun:初始化连接数据库的配置。
        """
        try:
            sqlEngine = create_engine("postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(**settingdict),client_encoding = 'utf8')
            self.dbConnection = sqlEngine.connect()
        except Exception as inst:
            print('Fail to connect postpresql')
            print(inst)
            logging.error('Fail to connect postpresql')
            logging.error(inst)

    def push_df(self,tablename,df):
        """
        param: tablename: 要增加数据的表名称
                sql    -->  demo:'''SELECT * FROM dat_energy.wzs_aircon_cop_hour;'''
        fun:将数据放入数据库。
        return:None 。
        """
        try:
            df.to_sql(name=tablename, con=self.dbConnection, if_exists = 'append', index=False)
        except Exception as inst:
            print('Fail to push data')
            print(inst)
            logging.error('Fail to push data')
            logging.error(inst)

    def fetch_df(self,sql):
        """
        param: sql    -->  demo:'''SELECT * FROM dat_energy.wzs_aircon_cop_hour;'''
        fun:从数据库拿取数据。
        return:返回选取的数据df,含栏位名称及值的类型。
        """
        try:
            df = pd.read_sql(sql,self.dbConnection)
        except Exception as inst:
            print('Fail to fetch data')
            print(inst)
            logging.error('Fail to fetch data')
            logging.error(inst)
       
        return df
    
    def close_connect(self):
        '''
        param :None。
        fun:关闭一个数据库连接。
        return:None。
        '''
        self.dbConnection.close()

class MyPymysql(object) :
    '''
    fun:使用pymysql与数据库通讯。
    '''
    def __init__(self,settingdict) :
        """
        param:settingdict --> {'user':ieuser,'password':iepwd,'host':iehost,'port':ieport,'database':iedb}
        fun:初始化连接数据库的配置。
        """
        try :
            self.MySqlConn = psycopg2.connect(**settingdict)
            self.MySqlConn.set_client_encoding('UTF8')
        except Exception as inst:
            print('Fail to connect postpresql')
            print(inst)
            logging.error('Fail to connect postpresql')
            logging.error(inst)

    def push_df(self,sqlheader,df):
        """
        param: sqlheader    -->  demo:'''insert INTO wmsdcagingdata VALUES (%s{}) ON conflict(labelid) DO NOTHING;'''.format(',%s'*11)   
               df   -->   一个DataFrame
        fun:往数据库插入数据。
        return:row 插入的行数。
        """
        with self.MySqlConn.cursor() as cursor:
            row = cursor.executemany(sqlheader,df.values.tolist())
            self.MySqlConn.commit()
        return row   

    def fetch_tuple(self,sql):
        """
        param: sql    -->  demo:'''SELECT * FROM dat_energy.wzs_aircon_cop_hour;'''
        fun:从数据库拿取数据。
        return:返回选取的数据,是一个元组的格式((onerow),……)。
        """
        try:
            with self.MySqlConn.cursor() as cursor:
                cursor.execute(sql)
                data = cursor.fetchall()
        except Exception as inst:
                print('Fail to fetch postpresql data')
                print(inst)
                logging.error('Fail to fetch postpresql data')
                logging.error(inst)

        return data

    def close_connect(self):
        '''
        param :None。
        fun:关闭一个数据库连接。
        return:None。
        '''     
        self.MySqlConn.close()

if __name__ == "__main__":
    print("开始测试")