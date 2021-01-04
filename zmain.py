#!/usr//bin/python
#-*- coding: utf-8 -*-

"""python3 自带函数功能"""
import re
import time
import datetime
import configparser

"""第三方库"""
import pandas as pd

"""自己写的函数功能"""
# import espipe
# import ftppipe
# import smbpipe
# import webpipe
# import sftppipe
# import mqttpipe
# import writelog
# import kafkapipe
import sendemail
import zmergedata
import mariadbpipe
import postpresqlpipe
# import excelcsvtxtpipe
from setting import productconfig

if __name__ == '__main__' :

    """create flow class that fun is fetching data or pushing data"""
    productconfig["db"]["mfgpgdb"]["database"] = "ZSAPFABRICP"
    productconfig["db"]["itmariadbw"]["db"] = "db_export_wzs"
    mfgpgdb = postpresqlpipe.MySqlalchemyPool(productconfig["db"]["mfgpgdb"])
    itmariadb = mariadbpipe.MyPymysql(productconfig["db"]["itmariadbw"])

    """create a class that fun is to merge data"""
    mymerge = zmergedata.MyMerge()

    """1、取得各种数据源"""
    sql = """SELECT * FROM wmsdcagingdata"""
    df = mfgpgdb.fetch_df(sql)
    print(df)

    """2、合并或者计算各种数据源"""
    df["tradate"] = df.tradate.apply(lambda x:x.strftime('%Y-%m-%d %H:%M:%S'))
    print(df)
    
    """3、将数据抛到指定位置"""
    sqlheader = '''replace INTO db_export_wzs.wmsdcagingdata VALUES (%s{}) ;'''.format(',%s'*11)
    itmariadb.push_df(sqlheader,df)

    """4、关闭各种连接"""
    mfgpgdb.close_connect()
    itmariadb.close_connect()


