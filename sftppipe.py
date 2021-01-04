#!/usr//bin/python
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
import paramiko

class MySftp(object):
    """
    fun:使用paramiko与SFTP通信。
    """
    def __init__(self,sftphost,sftpport,sftpuser,sftppw) :
        """
        param:sftphost --> SFTP主机地址
              sftpport --> SFTP主机端口
              sftpuser --> 登录SFTP账号
              sftppw -->   登录SFTP密码
        fun:初始化连接SFTP的配置。
        """

        if not os.path.exists('./sftptemporaryfile'):
            os.makedirs('./sftptemporaryfile')
        try :
            SFtp_transport = paramiko.Transport(sftphost,sftpport)
            SFtp_transport.connect(username = sftpuser,password = sftppw)
            self.SFtp_Conn = paramiko.SFTPClient.from_transport(SFtp_transport)
        except Exception as inst:
            print('Fail To Connect Sftp !')
            print(inst)
            logging.error('Fail To Connect Sftp !')
            logging.error(inst)
    
    def download_file(self,remotedri,filetpye,filestring = "."):
        """
        param:remotedri --> "/Spec/1LED00_OPR/AllPlant_QIS/"
              filetpye --> "xls"
              filestring = "." --> filestring = "DLA" 该文档具有的唯一特征。
        fun:从SFTP指定目录下，下载指定类型、指定特征的一个文档，到sftptemporaryfile文件夹。
        return: 返回下载的文档名称 :-->"NPI_PRINFO1.xls" onefile(包含filetype)
        """

        print('Start To Download {0}'.format(remotedri))
        logging.info('Start To Download {0}'.format(remotedri))

        Files = self.SFtp_Conn.listdir(remotedri)
        TotalFiles = 0
        SuccessFiles = 0
        for onefile in Files :
            try :
                filecompile = re.compile('{}'.format(filestring))
                findlength = len(filecompile.findall(onefile))
                if onefile.lower().endswith('.{}'.format(filetpye)) and findlength > 0:
                    TotalFiles+= 1
                    Remotepath = remotedri+'{0}'.format(onefile)
                    Localpath = './sftptemporaryfile/{0}'.format(onefile)
                    self.SFtp_Conn.get(Remotepath,Localpath)
                    SuccessFiles+= 1
            except Exception as inst :
                print('Fail To Download {0}'.format(onefile))
                print(inst)
        
        print("sftp download Total Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))
        logging.info("sftp download Total Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))

        return onefile

    def delete_file(self,remotedri,successpush) :
        """
        param:remotedri --> "/Spec/1LED00_OPR/AllPlant_QIS/"
              successpush --> "指定文件名称列表--> ["NPI_PRINFO1.xls","NPI_PRINFO2.xls","NPI_PRINFO3.xls"]
        fun:删除SFTP指定目录下指定文档列表。
        return: None
        """
        print('Start To delete {0}'.format(remotedri))
        logging.info('Start To delete {0}'.format(remotedri))
		
        for onefile in successpush :
            try :
                Remotepath = remotedri+'{0}'.format(onefile)
                self.SFtp_Conn.remove(Remotepath)
            except Exception as inst :
                print('Fail To delete {0} '.format(onefile))
                print(inst)
	
        print('Success To Delete SFtp File !')
        logging.info('Success To Delete SFtp File !')
