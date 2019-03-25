#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/25 16:14
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : sandglass_status_monitor.py
# @Software: PyCharm
# @Desc    :

import logging
import logging.handlers
import datetime
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from datetime import datetime
import time
import re
import hashlib
import subprocess


myhash = hashlib.md5()
format_dict = {
    logging.DEBUG: logging.Formatter('%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s'),
    logging.INFO: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.WARNING: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.ERROR: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s'),
    logging.CRITICAL: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s')
}


class Logger(object):
    __cur_logger = logging.getLogger()

    def __init__(self, loglevel):
        new_logger = logging.getLogger(__name__)
        new_logger.setLevel(loglevel)
        formatter = format_dict[loglevel]
        filehandler = logging.handlers.RotatingFileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sandglass_status_monitor.log'), mode='w', maxBytes=1024*1024)
        filehandler.setFormatter(formatter)
        new_logger.addHandler(filehandler)
        #create handle for stdout
        streamhandler = logging.StreamHandler()
        streamhandler.setFormatter(formatter)
        #add handle to new_logger
        new_logger.addHandler(streamhandler)
        Logger.__cur_logger = new_logger

    @classmethod
    def getlogger(cls):
        return cls.__cur_logger


logger = Logger(logging.DEBUG).getlogger()

sender = ''
receivers = ['']
smtp_server = 'smtp.dotcunited.com'
password = ''

sandglass_log_dir = ''


#获取文件MD5
def get_md5sum(file_name):
    file_path = os.path.join(sandglass_log_dir, file_name)
    cmd = 'md5sum {}'.format(file_path)
    proc = subprocess.Popen(args=cmd, shell=True, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    try:
        stdout, stderr = proc.communicate()
        if proc.returncode <> 0:
            logger.error('exec cmd:{} fail'.format(cmd))
            logger.error('stderr: {}'.format(stderr))
            return None
        return stdout.split(' ')[0]
    except Exception, e:
        logger.error('exec cmd:{} raise error:{}'.format(cmd, e))
        return None

#发邮件
class SendMail(object):
    @staticmethod
    def send_mail(subject, text):
        message = MIMEMultipart()
        message['From'] = Header(sender, 'utf-8')
        message['To'] = Header(','.join(receivers), 'utf-8')
        message['Subject'] = Header(subject, 'utf-8')
        # 邮件正文内容
        message.attach(MIMEText(text, 'plain', 'utf-8'))
        try:
            smtpObj = smtplib.SMTP_SSL(smtp_server, 465)
            smtpObj.login(sender, password)
            smtpObj.sendmail(sender, receivers, message.as_string())
            logger.info('send email success')
        except smtplib.SMTPException, ex:
            logger.error('send email fail: {}'.format(ex))

#得到runtask.log和scheduler.log
def check_file(file):
    prog = re.compile(r'^(runtask\.runtask|scheduler\.scheduler)\.\d{4}-\d{2}-\d{2}.log$')
    result = prog.match(file)
    return result

def check_status(file_infos):
    all = os.listdir(sandglass_log_dir)
    result = {'runtask':True, 'scheduler':True}
    # all = ['runtask.runtask.2019-03-18.log', 'scheduler.scheduler.2019-03-12.log', 'scheduler.scheduler.2019-03-11.log', 'runclean.runclean.2019-03-20.log']
    all_files = filter(check_file, all)
    runtask_files = [file for file in all_files if file.startswith('runtask')]
    scheduler_files = [file for file in all_files if file.startswith('scheduler')]
    runtask_files.sort(reverse=True)
    scheduler_files.sort(reverse=True)
    if runtask_files:
        md5_1 = get_md5sum(runtask_files[0])
        logger.info('file:{} md5:{}'.format(runtask_files[0], md5_1))
        if file_infos.get(runtask_files[0]):
            if md5_1 == file_infos.get(runtask_files[0]):
                result['runtask'] = False
        file_infos[runtask_files[0]] = md5_1
    if scheduler_files:
        md5_1 = get_md5sum(scheduler_files[0])
        logger.info('file:{} md5:{}'.format(scheduler_files[0], md5_1))
        if file_infos.get(scheduler_files[0]):
            if md5_1 == file_infos.get(scheduler_files[0]):
                result['scheduler'] = False
        file_infos[scheduler_files[0]] = md5_1
    return result


def main():
    file_infos = {}
    rerun_time = 1800
    while 1:
        startTime = datetime.now()
        ret = check_status(file_infos)
        logger.info('ret:{}'.format(ret))
        logger.info('file_infos:{}'.format(file_infos))
        if not ret.get('runtask') or not ret.get('scheduler'):
            now = startTime.strftime('%Y-%m-%d %H:%M:%S')
            # #发送邮件
            text = 'sandglass status check runtask:{runtask}, scheduler:{scheduler}, check in time please'.format(**ret)
            logger.debug('text:{}'.format(text))
            # SendMail.send_mail('sandglass status check email {}'.format(now), text)
            rerun_time+=36
        else:
            rerun_time = 18
        logger.debug('need sleep {}s'.format(rerun_time))
        time.sleep(rerun_time)
        endTime = datetime.now()
        logger.info('all seconds:{}'.format((endTime - startTime).seconds))


if __name__ == '__main__':
    main()