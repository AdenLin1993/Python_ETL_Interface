#!/usr//bin/python
#-*- coding: utf-8 -*-

"""python3 自带库"""
import re
import os
import time
import logging
import datetime

"""python 第三方库"""
from smbclient import(listdir,mkdir,register_session,rmdir,scandir,link,open_file,remove,stat,symlink,)

class Fetch(object) :
    """
    fun:使用smbclient与SMB通信。安装指令: pip install smbprotocol
    """
    def __init__(self,smbhost,smbuser,smbpw):
        """
        param:smbhost --> SMB主机地址
              smbuser --> 登录SMB账号
              smbpw -->   登录SMB密码
        fun:初始化连接SMB的配置。
        """
        if not os.path.exists('./smbtemporaryfile'):
            os.makedirs('./smbtemporaryfile')

        try :
            register_session(smbhost,smbuser,smbpw)
        except Exception as inst:
            print('Fail To Connect SMB !')
            print(inst)
            logging.error('Fail To Connect SMB !')
            logging.error(inst)

    def download_file(self,remotedri,filetpye,filestring = "."):
        """
        param:remotedri --> "10.41.52.124/mm/Scrap/output"
              filetpye --> "xlsx"
              filestring = "." --> filestring = "Scrap" 该文档具有的唯一特征。
        fun:从SMB指定目录下，下载指定类型、指定特征的一个文档，到smbtemporaryfile文件夹。
        return: 返回下载的文档名称 :-->"NPI_PRINFO1.xls" onefile(包含filetype)
        """

        print('Start To Download smbfile')
        logging.info('Start To Download smbfile')

        Files = listdir(remotedri)
        TotalFiles = 0
        SuccessFiles = 0
        for onefile in Files :
            try : 
                filecompile = re.compile('{}'.format(filestring))
                findlength = len(filecompile.findall(onefile))  
                if onefile.lower().endswith('.{}'.format(filetpye)) and findlength > 0:
                    Remotepath = os.path.join(remotedri,onefile)
                    Localpath = './smbtemporaryfile/{}'.format(onefile)
                    TotalFiles+= 1
                    with open_file(Remotepath,mode = "rb") as fr:
                        file_bytes = fr.read()
                    with open(Localpath,"wb") as fw :
                        fw.write(file_bytes)
                    SuccessFiles+= 1
            except Exception as inst :
                print('Fail To Download smbfile ')
                print(inst)

        print("smb download Total Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))
        logging.info("smb download Total Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))        
        
        return onefile

    def upload_file(self,remotedri,filetpye,filestring = "."):
        """
        param:remotedri --> "\\10.41.22.77\Plant3\Special\M360\MZ3I00\中台数据库-Ason\\"
              filetpye --> "xlsx"
              filestring = "." --> filestring = "Scrap" 该文档具有的唯一特征。
        fun:上传指定目录下，指定类型、指定特征的一个文档，到SMB指定目录下。
        return: 返回上传的文档名称 :-->"NPI_PRINFO1.xls" onefile(包含filetype)
        """

        print('Start To Upload file ！')
        logging.info('Start To Upload file ！')

        for _, _, files in os.walk('./smbtemporaryfile'):
            for onefile in files :
                try :
                    filecompile = re.compile('{}'.format(filestring))
                    findlength = len(filecompile.findall(onefile))
                    if onefile.lower().endswith('.{}'.format(filetpye)) and findlength > 0:
                        Remotepath = remotedri + onefile
                        Localpath = './smbtemporaryfile/{0}'.format(onefile)
                        with open(Localpath,mode = "rb") as fr:
                            file_bytes = fr.read()
                        with open_file(Remotepath,mode = "wb") as fw :
                            fw.write(file_bytes)    
                except Exception as inst :
                    print('Fail To Upload Smbfile !')
                    print(inst)

        print('Success To Upload Smbfile !')
        logging.info('Success To Upload Smbfile !')

        return onefile