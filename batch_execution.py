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
        generate_classifier_model(data, target, target_names=target_name, test_probability=0.3)
    return target_name

def do_classification(app_list, target_name={}):
    logger.info('do classification begin')
    if exists(result_xls):
        remove(result_xls)
    for app_name in app_list:
        record = get_record(app_name, 30)
        test_data, predicted = load_classifier_model(record, target_name)
        save_excel(result_xls, app_name, test_data, predicted, target_name)
    logger.info('do classification end')


def main():
    startTime = datetime.now()
    # app_list = ['com.cleanmaster.mguard']
    app_list = ['com.cleanmaster.mguard', 'com.hyperspeed.rocketclean', 'com.apps.go.clean.boost.master', 'com.colorphone.smooth.dialer', 'com.call.flash.ringtones', 'com.appconnect.easycall']
    #下载评论
    get_review(app_list)
    #做训练
    target_name = do_training()
    # target_name = {0: 'Bugs', 1: 'Customer Support', 2: 'Design & UX', 3: 'Dissatisfied users', 4: 'Feature Requests', 5: 'Satisfied users', 6: 'Security & Accounts', 7: 'Sign Up & Login', 8: 'Update'}
    #生成分类结果
    do_classification(app_list, target_name)
    #发送邮件
    SendMail.send_mail('review classification result email', '这是评论分类结果邮件……', result_xls)
    endTime = datetime.now()
    logger.info('all seconds:{}'.format((endTime - startTime).seconds))

if __name__ == '__main__':
    main()