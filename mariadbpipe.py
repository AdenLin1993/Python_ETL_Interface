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
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from DBUtils.PooledDB import PooledDB,SharedDBConnection

"""
#暂时收录三种连接mysql的方式，根据需求选择使用。

##1、SqlalchemyPipe类是使用sqlalchemy这个包建立一个连接池，适用于建立server、多线程时使用，python中有名的ORM工具。
    目前暂时将这个结合pandas使用,to_sql快速创建表结构。read_sql快速取得数据。(一次性使用的程式、或者只读程式)
    注意：连接的库名不要错了

##2、MyPymysql类是使用pymysql这个包进行单次连接，创建一个实例类即生成一个连接，一个实例类中的程式执行完毕即断开连接，
    测试发现单个实例类中连接时长会根据数据库设置默认连接多长时间无操作会自动断开。

##3、MyPymysqlPoll类是使用pymysql、DBUtils==1.3这两个包建立一个连接池，适用于建立server、多线程时使用。
"""

class MySqlalchemyPool(object) :
    """
    fun:使用sqlalchemy创建ORM工具与数据库通讯。
    """
    def __init__(self,settingdict) :
        """
        param:settingdict --> {'user':ieuser,'pwd':iepwd,'host':iehost,'port':ieport,'db':iedb}
        fun:初始化连接数据库的配置。
        """
        try:
            sqlEngine = create_engine("mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}?charset=utf8".format(**settingdict))
            self.dbConnection = sqlEngine.connect()
        except Exception as inst:
            print('Fail to connect mariadb')
            print(inst)
            logging.error('Fail to connect mariadb')
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
        param:settingdict --> {'user':ieuser,'pwd':iepwd,'host':iehost,'port':ieport,'db':iedb}
        fun:初始化连接数据库的配置。
        """
        try :
            self.MySqlConn = pymysql.connect(**settingdict,charset='utf8')
        except Exception as inst:
            print('Fail to connect mariadb')
            print(inst)
            logging.error('Fail to connect mariadb')
            logging.error(inst)

    def push_df(self,sqlheader,df):
        """
        param: sqlheader    -->  demo:'''REPLACE INTO Training5.wzs_export_acsminfo VALUES (%s{});'''.format(',%s'*19)   
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
                print('Fail to fetch mariadb data')
                print(inst)
                logging.error('Fail to fetch mariadb data')
                logging.error(inst)

        return data

    def execute_sql(self,sql):
        """
        param: sql    -->  demo:'''DELETE FROM db_export_wzs.wmsdcagingdata
                                    WHERE tradate = '2020-12-21 21:51:21';'''
        fun:执行指定的SQL语句。
        return:返回一个SQL语句执行的状态 execute_status = 0 execute_status = 1表示执行失败。
        """
        execute_status = 0
        try:
            with self.MySqlConn.cursor() as cursor:
                cursor.execute(sql)
                self.MySqlConn.commit()
        except Exception as inst:
                execute_status = 1
                print('Fail to execute mariadb data')
                print(inst)
                logging.error('Fail to execute mariadb data')
                logging.error(inst)

        return execute_status

    def close_connect(self):
        '''
        param :None。
        fun:关闭一个数据库连接。
        return:None。
        '''     
        self.MySqlConn.close()

class MysqlPool(object):
    '''
    fun:使用pymysql、DBUtils==1.3建立连接池与数据库通讯。
    '''
    def __init__(self,settingdict) :
        """
        param:settingdict --> {'user':ieuser,'pwd':iepwd,'host':iehost,'port':ieport,'db':iedb}
        fun:初始化连接数据库的配置。
        """
        self.POOL = PooledDB(
            creator=pymysql,     # 使用链接数据库的模块
            maxconnections=3,    # 连接池允许的最大连接数，0和None表示不限制连接数
            mincached=1,         # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
            maxcached=1,         # 链接池中最多闲置的链接，0和None不限制
            maxshared=3,         # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
            blocking=True,       # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            maxusage=None,       # 一个链接最多被重复使用的次数，None表示无限制
            setsession=[],       # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
            ping=0,              # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
            host=settingdict['host'],
            port=settingdict['port'],
            user=settingdict['user'],
            password=settingdict['pwd'],
            database=settingdict['db'],
            charset='utf8'
        )

    def connect(self):
        '''
        param:None。
        fun:启动连接
        return:None。
        '''
        conn = self.POOL.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        return conn, cursor

    def push_df(self,sqlheader,df):
        """
        param: sqlheader    -->  demo:'''REPLACE INTO Training5.wzs_export_acsminfo VALUES (%s{});'''.format(',%s'*19)   
               df   -->   一个DataFrame
        fun:往数据库插入数据。
        return:row 插入的行数。
        """
        conn, cursor = self.connect()
        row = cursor.executemany(sqlheader,df.values.tolist())
        conn.commit()
        self.connect_close(conn, cursor)
        
        return row

    def fetch_tuple(self,sql):
        """
        param: sql    -->  demo:'''SELECT * FROM dat_energy.wzs_aircon_cop_hour;'''
        fun:从数据库拿取数据。
        return:返回选取的数据,是一个元组的格式((onerow),……)。
        """
        conn, cursor = self.connect()
        cursor.execute(sql)
        data = cursor.fetchall()
        self.connect_close(conn, cursor)

        return data

    def close_connect(self,conn, cursor):
        '''
        param:conn, cursor
        fun:关闭连接
        return:None。
        '''
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("开始测试！")