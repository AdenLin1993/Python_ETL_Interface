#!/usr//bin/python
#-*- coding: utf-8 -*-

"""python3 自带库"""
import os
import io
import re
import sys
import time
import logging
import datetime

"""python 第三方库"""
from ftplib import FTP            

class MyFtp(object):
    """
    fun:使用ftplib与FTP通信。
    """
    def __init__(self,ftphost,ftpport,ftpuser,ftppw) :
        """
        param:ftphost --> FTP主机地址
              ftpport --> FTP主机端口
              ftpuser --> 登录FTP账号
              ftppw -->      登录FTP密码
        fun:初始化连接FTP的配置。
        """
        if not os.path.exists('./ftptemporaryfile'):
            os.makedirs('./ftptemporaryfile')

        try :
            self.FtpClient = FTP()
            self.FtpClient.connect(ftphost,ftpport)
            self.FtpClient.login(ftpuser,ftppw)
        except Exception as inst :
            print('Fail To Connect Ftp ！')
            print(inst)
            logging.error('Fail To Connect Ftp ！')
            logging.error(inst)

    def download_file(self,remotedri,filetpye,filestring = ".") :
        """
        param:remotedri --> "/Man_Power/Training_Roadmap"
              filetpye --> "xlsx"
              filestring = "." --> filestring = "DLA" 该文档具有的唯一特征。
        fun:从FTP指定目录下，下载指定类型、指定特征的一个文档，到ftptemporaryfile文件夹。
        return: 返回下载的文档名称 :-->"NPI_PRINFO1.xls" onefile(包含filetype)
        """

        print('Start To Download {0}'.format(remotedri))
        logging.info('Start To Download {0}'.format(remotedri))

        self.FtpClient.cwd(remotedri)
        files = self.FtpClient.nlst()
        TotalFiles = 0
        SuccessFiles = 0
        for onefile in files :
            try :
                filecompile = re.compile('{}'.format(filestring))
                findlength = len(filecompile.findall(onefile))
                if onefile.lower().endswith('.{}'.format(filetpye)) and findlength > 0:
                    TotalFiles+= 1
                    TmpPath = './/ftptemporaryfile//{0}'.format(onefile)
                    self.FtpClient.retrbinary("RETR "+onefile,open(TmpPath,'wb').write)
                    SuccessFiles+= 1
            except Exception as inst :
                print(' Fail To Download {0}'.format(onefile))
                print(inst)

        print("ftp downloadTotal Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))
        logging.info("ftp downloadTotal Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))
        
        return onefile

    def close_connect(self):
        '''
        param :None。
        fun:关闭一个FTP连接。
        return:None。
        '''
        self.FtpClient.quit()