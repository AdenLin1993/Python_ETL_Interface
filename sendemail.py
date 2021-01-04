#!/usr/bin/python
#-*- coding: utf-8 -*-

"""第三方库"""
import smtplib
import pandas as pd
from email.header import Header
from email.utils import make_msgid
from email.mime.text import MIMEText
from email.message import EmailMessage

def push_mail(self,projectname,failinfo):
	'''
	param: projectname：--> 程式是哪个案子的
			failinfo ：--> 通知信息
	fun:在程式中调用这个函数，发邮件通知程式中的异常或者其他信息，只能在指定的服务器才能运行。
	return: None
	'''

	message = """
	<html>
		<head></head>
		<body>
			<p style="font-size:30;color:#FF0000">{} fail</p>
			<p>Dear Carey,</p>
			<p>{}</p>
			<br/>
		</body>
	</html>""".format(projectname,failinfo)

	msg = EmailMessage()
	msg['From'] = 'DAT_Announcement@Wistron.com'
	msg['To'] = "Carey_Lin@wistron.com"
	msg['Subject'] = '{}'.format(projectname)
	msg.add_alternative(message, subtype='html')
	with smtplib.SMTP('wzsowa.wistron.com') as s:
		s.set_debuglevel(1)
		s.send_message(msg)

if __name__ == '__main__':
	projectname = "test"
	failinfo = "你的程式报错了"
	push_mail(projectname,failinfo)