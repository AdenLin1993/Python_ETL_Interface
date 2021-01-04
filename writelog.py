#!/usr//bin/python
#-*- coding: utf-8 -*-

"""python 自带库"""
import os
import logging
import datetime

def __init__():    
    """
    parameter:None
    fun:导入writelog后初始化logging设置
    return:None
    """

    if not os.path.exists('./Log') :
        os.makedirs('./Log')

    logging.basicConfig(filename='./Log/'+datetime.datetime.today().strftime("%Y%m%d")+'.log'
        , level=logging.INFO
        , format='%(asctime)s %(message)s'
        , datefmt='%Y/%m/%d %I:%M:%S %p')
