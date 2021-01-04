#!/usr//bin/python
#-*- coding: utf-8 -*-

"""python3 自带函数功能"""
import os
import re																				
import io
import sys
import time	
import copy
import json								
import logging
import datetime	

"""第三方库"""
from elasticsearch import helpers
from elasticsearch import Elasticsearch		

"""自己写的函数功能"""
import writelog

class MyEs(object) :
	"""
	fun：使用第三方库与es数据库通讯。
	"""
	def __init__(self,settingdict) :
		"""
	    parameter:None。
    	fun:初始化与ES数据库的通讯连接。
    	return:None。
		"""
		try :
			self.EsClient = Elasticsearch(settingdict,maxsize = 25,timeout = 180)
		except Exception as inst :
			print('Fail to connect elasticsearch db !')
			print(inst)

	def fetch_page(self,Index,query_body,size=100000):
		"""
	    parameter:Index  --> ["wzs_bms_tb2_ch_history_*"]
				  query_body -->  {
									"query": {
										"bool" : {
											"must" : [
												{"range" :{
														"evt_dt" : {
															"from" : 1608505200000,"to" : 1608678000000,"include_lower" : True,"include_upper":True
														}
													}
												},
												{
													"match_phrase": {
														"building": {
															"query": "TB2",
															"slop": 0,
															"boost": 1
														}
													}
												},
												{
													"match_phrase": {
														"machine_type": {
															"query": "CH",
															"slop": 0,
															"boost": 1
														}
													}
												}
											]
										}
									}
								}
				size=100000 --> 一次性请求拿取数据的的量
    	fun:按照query_body 从es数据库拿取数据，电量的数据size可以使用默认为100000条记录，其他数据不能拿取时，size取10000。
    	return:返回一个可迭代对象page_list，和这个可迭代对象的数据数量page_qty。
		"""

		print('\n Start to fetch page!\n')
		logging.info('\n Start to fetch page!\n')
		try:
			page = self.EsClient.search(index=Index, size=size, body=query_body, scroll='50m')
			page_list = page["hits"]["hits"]
			page_qty = page["hits"]["total"]
		except Exception as inst:
			print("Fail to fetch page!")
			print(inst)

		print('\n Success to fetch page!\n')
		logging.info('\n Success to fetch page!\n')

		return page_list,page_qty

	def push_listdict(self,listdict,Index,indextype):
		"""
	    parameter:listdict --> [{},……],每个字典中的数字类型统一为float或者int,转为json不容易出错。
				  Index --> 索引 'fem_aircomp_power_2020.09'
				  indextype --> 索引下面的类型 "energy"
    	fun:将一个包含很多字典的列表推送至es指定索引及类型内。
    	return:None。
		"""
		print('Start to push lisdict to esdb ！')
		logging.info('Start to push lisdict to esdb ！')

		try:
			actions = []
			for onedict in listdict:
				result = json.dumps(onedict)
				action = {'_op_type':'index', 
					'_index':Index,
					'_type':indextype,
					'_source':result}
				actions.append(action)
			"""将数据传送至es"""
			helpers.bulk(client = self.EsClient,actions = actions)
		except Exception as inst:
			print("Fail to push lisdict to esdb !")
			print(inst)			

		print('Success to push lisdict to esdb ！')
		logging.info('Success to push lisdict to esdb ！')

	def delete_listdict(self,listdict,Index,indextype):
		"""
	    parameter:listdict --> [{},……] ,要删除记录的总列表，每条记录都是ES上原来的数据（必须包含_id和_source）。
				  Index --> 索引 'fem_aircomp_power_2020.09'
				  indextype --> 索引下面的类型 "energy"
    	fun:删除es上指定索引指定类型内的记录。
    	return:None。
		"""
		print('Start to delete lisdict in esdb ！')
		logging.info('Start to delete lisdict in esdb ！')
		
		try:
			for onedict in listdict:
				action = {'_op_type':'delete',  
					'_index':Index,
					'_type':indextype,
					'_id': onedict["_id"],
					'_source':onedict["_source"]}
				actions = []
				actions.append(action)
				helpers.bulk(client = self.EsClient,actions = actions)
		except Exception as inst:
			print("Fail to delete lisdict in esdb !")
			print(inst)	

		print('Success to delete lisdict in esdb ！')
		logging.info('Success to delete lisdict in esdb ！')

if __name__ == "__main__":
	print("测试开始！")