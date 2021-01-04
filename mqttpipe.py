#!/usr/bin/python
#-*- coding: utf-8 -*-

"""python3 自带库"""
import re
import os                        
import io
import sys                            
import time
import logging                    
import datetime                                        

"""python 第三方库"""                                        
import paho.mqtt.publish as publish
import paho.mqtt.subscribe as subscribe

class MyMqtt(object) :
    """
    fun:从不用验证的mqtt服务接取数据
    """
    def download_data(self,topic,settingdict):
        """
        param: topic --> "WZS/DA/Energy/TB2/aircomp/combination"
                settingdict  --> {'hostname':'zsarm-mqtt-p03.wzs.wistron','port':9001}
        fun:从Mqtt下载指定主题的内容。
        return: onemessage --> 返回监听主题最新的一条记录的payload 及data部分。
        """
        print('Start To Download Data From Mqtt ！')
        logging.info('Start To Download Data From Mqtt ！')

        onemessage = subscribe.simple(topic,**settingdict,msg_count=1,client_id="carey_lin", keepalive=60,transport="websockets").payload

        print('Fail To Download Data From Mqtt ！')
        logging.info('Fail To Download Data From Mqtt ！')

        return onemessage

    def upload_data(self,messagesdict,settingdict):
        """
        param:  messagesdict  由主题和信息组成的字典，-->{"WZS/DA/Energy/TB2/aircomp/combination":[onemassage,……],……}
                 "WZS/DA/Energy/TB2/aircomp/combination"是主题topic,每次信息上传的信息为onemassage(json格式)
                 settingdict  --> {'hostname':'zsarm-mqtt-p03.wzs.wistron','port':9001}
        fun:将数据上传到指定的mqtt主题上。
        return: None
        """

        print('Start To Upload Data To Mqtt ！')
        logging.info('Start To Upload Data To Mqtt ！')

        msgs = []
        for topic in messagesdict:
            for onemessage in messagesdict[topic]:
                msg = {'topic':0, 'payload':0, 'retain':True}
                msg['topic'] = topic
                msg['payload'] = onemessage
                msgs.append(msg)
        try:
            publish.multiple(msgs,**settingdict,client_id="carey_lin", keepalive=60,transport="websockets")
        except Exception as inst :
            print("Fail To Upload Data To Mqtt！")

        print('Success To Upload Data To Mqtt ！')
        logging.info('Success To Upload Data To Mqtt !')


