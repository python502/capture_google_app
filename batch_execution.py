#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/5/10 14:49
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : batch_execution.py
# @Software: PyCharm
# @Desc    :
from capture_google_review import CaptureGoogleReview
from comment_classification import *
from send_email import SendMail
from datetime import datetime

useragent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
objCaptureGoogleReview = CaptureGoogleReview(useragent)


def get_review(app_list):
    for app_name in app_list:
        logger.info('{} get review begin'.format(app_name))
        objCaptureGoogleReview.deal_main(app_name)
        logger.info('{} get review end'.format(app_name))

def do_training(flag = False):
    data, target, target_name = load_data(r'D:\capture_google_app\appbot')
    if flag:
        pass_rate = generate_classifier_model(data, target, target_names=target_name, test_probability=0.3)
        if not pass_rate:
            raise ValueError('pass rate is too low')
    return target_name

def do_classification(app_list, xls_file=None, target_name={}):
    logger.info('do classification begin')
    if xls_file and exists(xls_file):
        remove(xls_file)
    for app_name in app_list:
        #从数据库中查询最近7天的数据
        record = get_record(app_name, 7)
        test_data, predicted = load_classifier_model(record, target_name)
        if xls_file:
            save_excel(xls_file, app_name, test_data, predicted, target_name)
    logger.info('do classification end')


def main():
    startTime = datetime.now()
    now = startTime.strftime('%Y%m%d%H%M%S')
    now1 = startTime.strftime('%Y-%m-%d %H:%M:%S')
    app_list = ['com.cleanmaster.security', 'com.cleanmaster.mguard', 'com.hyperspeed.rocketclean', 'com.apps.go.clean.boost.master', 'com.colorphone.smooth.dialer', 'com.call.flash.ringtones', 'com.appconnect.easycall']
    #下载评论
    get_review(app_list)
    #做训练
    target_name = do_training()
    # target_name = {0: 'Bugs', 1: 'Customer Support', 2: 'Design & UX', 3: 'Dissatisfied users', 4: 'Feature Requests', 5: 'Satisfied users', 6: 'Security & Accounts', 7: 'Sign Up & Login', 8: 'Update'}
    #生成分类结果
    xls_file = result_xls.format(now)
    do_classification(app_list, xls_file, target_name)
    logger.info('classification result file: {}'.format(xls_file))
    #发送邮件
    SendMail.send_mail('review classification result email {}'.format(now1), '这是评论分类结果邮件……', xls_file)
    endTime = datetime.now()
    logger.info('all seconds:{}'.format((endTime - startTime).seconds))

if __name__ == '__main__':
    main()