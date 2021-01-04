#!/usr/bin/python
#-*- coding: utf-8 -*-

"""python3 自带库"""
import re
import os

"""python 第三方库"""
import requests
from requests_ntlm import HttpNtlmAuth

class MyWeb(object) :
    """
    fun:从各种Web服务接取数据
    """
    def __init__(self,user,password):
        """
        param: user --> 登录web系统的用户名
                password  --> 登录web系统的密码
        fun:初始化web登录
        return: None
        """

        if not os.path.exists('./temporaryfile'):
            os.makedirs('./temporaryfile')

        try:
            self.auth = HttpNtlmAuth(user,password)
        except Exception as inst:
            print("Fail To Login Web !")

    def download_data(self): 
        """
        param: 从EIP系统抓取相关信息
        fun:从EIP系统下载对应的信息到指定文件夹。
        return: None
        """
        print("Start To Download Data !") 

        try :
            """ fa3a報表的路徑 """
            fa3a_dri = "https://mfgkm-wzs.wistron.com/P3A-War_room/DocLib1/WZS%20Plant3%20Final%20assembly%20Schedule-MZ3100(Building%203A)"
            resp_dri = requests.get(fa3a_dri,auth=self.auth,verify = False)
            dri_content = resp_dri.content.decode("utf-8")
            filepat = re.compile("id=/P3A-War_room/DocLib1/.*?\d\d\d\d\d\d[A,B,C].xlsx")
            filelist = filepat.findall(dri_content)
            DATELIST = []
            datepat = re.compile("\d\d\d\d\d\d")
            for onedate in filelist :
                date = datepat.findall(onedate)[0]
                DATELIST.append(date)
            LAST_DATE = max(DATELIST)

            for onedate in filelist :
                date = datepat.findall(onedate)[0]
                if date == LAST_DATE :
                    filename_content = onedate
            filenamepat = re.compile("P3A-War_room/DocLib1/.*?\d\d\d\d\d\d[A,B,C].xlsx")
            filename = filenamepat.findall(filename_content)[0]
            url = "https://mfgkm-wzs.wistron.com/{}".format(filename)
            
            resp = requests.get(url,auth=self.auth,verify=False)
            filecontent = resp.content
            with open ('./temporaryfile/schedule.xlsx','wb') as f:
                    f.write(filecontent)
        except Exception as inst :
            print("Fail To Download Data !")
        
        print("Success To Download Data !")