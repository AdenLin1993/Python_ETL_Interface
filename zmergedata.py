#!/usr//bin/python
#-*- coding: utf-8 -*-

"""python 自带库"""
import os                                       
import io                               
import re 
import sys                                           
import copy
import time   
import logging                                         
import datetime                                        
import configparser                                         

"""第三方库""" 
import numpy as np
import pandas as pd                                            

class MyMerge(object) :
    """
    fun:进行数据的合并、变形。
    """
        
    def merge_combination(self,combinationdict,coprank,load_rate,RT,cur_hour_time,cur_time):
        '''
        param: combinationdict : {'550RT': 2.0, '800RT': 1.0},
                coprank : (('550RT','CH-1',1),('550RT','CH-2',2),……),
                load_rate :{'550RT': 0.6022690537296598, '800RT': 0.6202354216856267}
                RT : 1158.6766
                cur_hour_time :现在整点时间戳 -->1607998000000
                cur_time : 现在时间戳 -->1607998494000
        fun:合并传入的参数为一个字典。
        return: 返回一个包含字典的列表  -->  [{},……]。
        '''

        init_com = {"site":"WZS","pub_by":"DA","api":"Energy","building":"TB2","project":"air_conditioner","insert_evt_dt": 0,
         "data": {"type" : "combination", "CH_1": 0, "CH_2": 0, "CH_3": 0, "CH_4": 0,"CH_5": 0, "CH_6": 0,
         "recommend_cold_qty":0,"550RT":0,"800RT":0,"recommend_waterouttem":"NaN","evt_dt": 0}}

        cur_hour_time = cur_hour_time * 1000
        cur_time = cur_time * 1000

        init_com["insert_evt_dt"] = cur_time
        init_com["data"]["evt_dt"] = cur_hour_time
        init_com["data"]["recommend_cold_qty"] = round(float(RT),4)
        init_com["data"]["550RT"] = round(float(load_rate["550RT"]),4)
        init_com["data"]["800RT"] = round(float(load_rate["800RT"]),4)

        """获取开机的冰机列表"""
        open_chs = []
        for onekey in combinationdict:
            opchqty = int(combinationdict[onekey])
            for i in range(opchqty):
                need_rank = i+1
                for onecoprank in coprank:
                    if  onecoprank[0] == onekey and onecoprank[2] == need_rank:
                        opench = onecoprank[1]
                        open_chs.append(opench)
        
        """赋值开机状态"""
        for oneopench in open_chs:
            number = oneopench.split('-')[1]
            key = 'CH_{}'.format(number)
            init_com["data"][key] = 1
        
        com_listdict = []
        com_listdict.append(init_com)

        return com_listdict
