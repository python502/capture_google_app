#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/25 16:14
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : sandglass_status_monitor.py
# @Software: PyCharm
# @Desc    : 通过check runtask.log和scheduler.log的MD5值是否改变，从而判断进程是否僵死，后期可加入kill进程代码

import logging
import logging.handlers
import datetime
import os
import json
from datetime import datetime
import requests
import re
import hashlib
import subprocess

current_dir = os.path.dirname(os.path.abspath(__file__))
myhash = hashlib.md5()
md5_file = os.path.join(current_dir, 'md5file.txt')
s = requests.session()

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
        filehandler = logging.handlers.RotatingFileHandler(os.path.join(current_dir, 'sandglass_status_monitor.log'), mode='a', maxBytes=1024*1024)
        filehandler.setFormatter(formatter)
        new_logger.addHandler(filehandler)
        Logger.__cur_logger = new_logger

    @classmethod
    def getlogger(cls):
        return cls.__cur_logger


logger = Logger(logging.INFO).getlogger()

sandglass_log_dir = '/home/avazu/channel/sandglass/sandglass/cron/log'

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

#得到runtask.log和scheduler.log
def check_file(file):
    prog = re.compile(r'^(runtask\.runtask|scheduler\.scheduler)\.\d{4}-\d{2}-\d{2}.log$')
    result = prog.match(file)
    return result

#将json.loads返回的unicode类型转成内置的str类型
def convert(input):
    if isinstance(input, dict):
        return {convert(key): convert(value) for key, value in input.iteritems()}
    elif isinstance(input, list):
        return [convert(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input

def read_md5(file):
    try:
        with open(file, 'r') as fd:
            line = fd.readline()
            return json.loads(line, object_hook=convert)
    except Exception, e:
        logger.error('read file:{} error:{}'.format(file, e))
        return {}

def write_md5(file, write_str):
    try:
        with open(file, 'w') as fd:
            fd.write(write_str)
        return True
    except Exception, e:
        logger.error('write file:{} write_str:{} error:{}'.format(file, write_str, e))
        return False

#check md5值是否改变
def check_status():
    result = {'runtask': True, 'scheduler': True}
    all = os.listdir(sandglass_log_dir)
    all_files = filter(check_file, all)
    runtask_files = [file for file in all_files if file.startswith('runtask')]
    scheduler_files = [file for file in all_files if file.startswith('scheduler')]
    runtask_files.sort(reverse=True)
    scheduler_files.sort(reverse=True)
    log_md5_info = {}
    if not runtask_files or not scheduler_files:
        logger.error('runtask_files len:{}, scheduler_files len:{}'.format(runtask_files, scheduler_files))
        result['runtask'] = True if runtask_files else False
        result['scheduler'] = True if scheduler_files else False
        return result

    md5_1 = get_md5sum(runtask_files[0])
    log_md5_info[runtask_files[0]] = md5_1

    md5_2 = get_md5sum(scheduler_files[0])
    log_md5_info[scheduler_files[0]] = md5_2

    logger.info('now_md5s: {}'.format(log_md5_info))
    before_md5s = read_md5(md5_file)
    if before_md5s:
        logger.info('bef_md5s: {}'.format(before_md5s))
        for key, value in log_md5_info.iteritems():
            if value == before_md5s.get(key, None):
                result[key.split('.')[0]] = False
    write_md5(md5_file, json.dumps(log_md5_info))
    return result

def send_alert(msg):
    url = "http://api.monitor.avazu.net/alert?name=dc.datax.sandglass.worker!&success=no&msg={}".format(msg)
    url = url.replace(' ', '%20') # replace whitespace by '%20'
    res = s.get(url)
    if res.status_code == 200:
        logger.info('send alert success,url:{}'.format(url))
        return True
    else:
        logger.error('send alert fail,res:{}'.format(res))
        return False

def main():
    logger.info('************************ Begin check sandglass status ************************')
    startTime = datetime.now()
    ret = check_status()
    logger.info('ret:{}'.format(ret))
    if False in ret.values():
        ret['now'] = startTime.strftime('%Y-%m-%d %H:%M:%S')
        msg = '{now} sandglass status check runtask:{runtask}, scheduler:{scheduler}, check it in time please'.format(**ret)
        logger.debug('msg: {}'.format(msg))
        send_alert(msg)
    endTime = datetime.now()
    logger.info('all seconds:{}'.format((endTime - startTime).seconds))
    logger.info('************************ End check sandglass status ************************')

if __name__ == '__main__':
    main()