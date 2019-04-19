# -*- coding:utf-8

import imaplib, email, os
import pandas as pd
import numpy as np
from datetime import datetime
from email.header import decode_header
import re

#sys.setdefaultencoding('utf8')

#连接到qq企业邮箱，其他邮箱调整括号里的参数
conn = imaplib.IMAP4_SSL("imap.exmail.qq.com", 993)
#用户名、密码，登陆
conn.login("jiayin.shi@yooli.com","rTfi5S6gNTezFffM")
#选定一个邮件文件夹
#可以用conn.list()查看都有哪些文件夹。中文的文件夹名称可能是乱码，没关系，直接拷贝过来就行了。

conn.select()

#提取了文件夹中所有邮件的编号，search功能在本邮箱中没有实现……
resp, mails = conn.search(None,'ALL')

#提取了指定编号（最新一封）的邮件
list_data = []
for i in range(len(mails[0].split())):
    resp, data = conn.fetch(mails[0].split()[i],'(RFC822)')
    emailbody = data[0][1]
    mail = email.message_from_bytes(emailbody)

#resp, data = conn.fetch (mails[0].split()[len(mails[0].split())-1],'(RFC822)')
# emailbody = data[0][1]
# mail = email.message_from_bytes(emailbody)

    fileName = '没有找到任何附件！'
    #获取邮件附件名称
    for part in mail.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
        fileName = part.get_filename()
    #如果文件名为纯数字、字母时不需要解码，否则需要解码
        try:
            fileName = decode_header(fileName)[0][0].decode(decode_header(fileName)[0][1])
        except:
            pass
    #如果获取到了文件，则将文件保存在制定的目录下
        if fileName != '没有找到任何附件！' and not re.search(("充值失败.xlsx|充值回访记录.xlsx"), fileName) is None:
            filePath = os.path.join("D:\\output", fileName)
            if not os.path.isfile(filePath):
                fp = open(filePath, 'wb')
                fp.write(part.get_payload(decode=True))
                fp.close()
                print ("附件已经下载，文件名为：" + fileName)
            else:
                print ("附件已经存在，文件名为：" + fileName)
conn.close()
conn.logout()


