#!/usr/bin/python
#-*- coding: utf-8 -*-

"""python 自带库"""
import re
import os							
import io	
import sys						
import time											
import logging	
import datetime						
import configparser											

"""第三方库"""
import xlrd
import xlsxwriter
import pandas as pd

class MyExcel(object) :
	"""
	fun:读写指定csv、txt文档的操作。
	"""
	def findpath(self,locatedri,filetpye,filestring = "."):
		'''
		param: locatedri  文件路径：--> './temporaryfile'
				filetpye  文件后缀：--> 'xlsx'、'xls'、'csv' 或者 'txt'
				filestring = "." ：--> 默认所有字符的文件。"de"
		fun:读取某文件的路径，参数确定的规则要能确定一个唯一的文档。
		return: 返回该唯一文档的一个路径名称：--> './temporaryfile/demo.xlsx'
		'''
		print('findpath {0} Start'.format(filestring))

		TotalFiles = 0
		SuccessFiles = 0
		for root, _, files in os.walk('{}'.format(locatedri)): 
			for onefile in files:
				try :
					filecompile = re.compile('{}'.format(filestring))
					findlength = len(filecompile.findall(onefile))
					if onefile.lower().endswith('.{}'.format(filetpye)) and findlength > 0:
						TotalFiles+= 1
						filepath = os.path.join(root, onefile)
						SuccessFiles+= 1
				except Exception as inst:
					print("find filepath fail")
					print(inst)
		print("Total Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))
		return filepath

	def getdataframe(self,filepath,sheetname = "sheet1") :
		'''
		param: filepath  文件路径：--> './temporaryfile/demo.csv'等其他格式
				sheetname = "sheet1" 工作簿的名称： --> 默认'sheet1'
		fun:读取某个excel文档某个sheet。
		return: 返回某个文档某个sheet的内容。
		'''
		print('getdataframe {0} Start'.format(sheetname))

		filetype = (filepath.split('.')[2]).lower()
		print(filepath,filetype)

		if filetype == 'xlsx':
			excel_reader = pd.ExcelFile(filepath)
			sheet_names = excel_reader.sheet_names
			filecompile = re.compile('{}'.format(sheetname))
			
			targetsheet = []
			for i in range(len(sheet_names)) :
				findlength = len(filecompile.findall(sheet_names[i].strip()))
				if findlength > 0 :
					targetsheet.append(i)
			df = pd.read_excel(filepath,targetsheet[0],header = 0)
		
		elif filetype == 'csv':
			excel_reader = pd.ExcelFile(filepath)
			sheet_names = excel_reader.sheet_names
			filecompile = re.compile('{}'.format(sheetname))
			
			targetsheet = []
			for i in range(len(sheet_names)) :
				findlength = len(filecompile.findall(sheet_names[i].strip()))
				if findlength > 0 :
					targetsheet.append(i)
			df = pd.read_excel(filepath,targetsheet[0],header = 0)

		elif filetype == 'txt':
			df = pd.read_table(filepath,sep='\t')

		return df
