#!/usr//bin/python
#-*- coding: utf-8 -*-

"""python3 自带库"""
import os
import time
import logging
import datetime

"""python 第三方库"""
import numpy as np
import pandas as pd
from bson import json_util, ObjectId
from confluent_kafka import KafkaError,Consumer,KafkaException
from bson.json_util import dumps,loads,JSONOptions,DEFAULT_JSON_OPTIONS

class MyKafkaDislogin(object) :
    """
    fun:从未不用认证的kafka服务主机接取数据
    """
    def __init__(self,servers,id,theme) :
        """
        param:servers --> '10.41.241.6:9092' 主机加端口
              id --> 监听程式名称 'wzs.pmo.p3.careyfetch'
              theme --> 监听所在主题 是一个列表 ['wzs.wigps.seccard']  
        fun:初始化监听Kafka的配置。
        """

        try :
            Source_Kafka_Consumer = Consumer({
                    'bootstrap.servers':servers
                    ,'group.id':id
                    ,'auto.offset.reset':'earliest'
                    , 'session.timeout.ms': 6000
                    })
            DEFAULT_JSON_OPTIONS.strict_uuid = True
            Source_Kafka_Consumer.subscribe(theme)
            self.consumer = Source_Kafka_Consumer
        except Exception as inst:
            print('Fail To Connect Kafaka !')
            print(inst)
            logging.error('Fail To Connect Kafaka !')
            logging.error(inst)

    def download_data(self):
        """
        param:None
        fun:从Kafa下载指定主题的内容，每个程式id名称只能拿取一次数据，这个数据是属于现在到7天前的数据。
        return: onelist --> 返回一个列表，包含7天前的数据。
        """
        print('Start To download kafaka data !')
        logging.info('Start To download kafaka data !')

        try:
            onelist = []
            count = 0
            while True:
                msg = self.consumer.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    print('Consumer error: {}'.format(msg.error()))
                    continue
                data = json_util.loads(msg.value())
                onelist.append(data)
                count = count + 1
                if count == 1000 :
                    break
        except Exception as inst:
                print('Fail To download kafaka data !')
                print(inst)
                logging.error('Fail To download kafaka data !')
                logging.error(inst)

        return onelist

    def close_connect(self):
        '''
        param:None
        fun:关闭连接
        return:None
        '''
        self.consumer.close()