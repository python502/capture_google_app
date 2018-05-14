#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/4/17 17:24
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : capture_football.py
# @Software: PyCharm
# @Desc    :
#http://goal.sports.163.com/schedule/20170921.html
import hashlib
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from MysqldbOperate import MysqldbOperate

DICT_MYSQL = {'host': '127.0.0.1', 'user': 'root', 'passwd': '111111', 'db': 'capture', 'port': 3306}
class CaptureFootBall(object):
    HEADER_GET = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Host:goal.sports.163.com
            User-Agent:{}
            '''

    TABLE_NAME_REVIEW = 'record_foot'
    def __init__(self, user_agent):
        self.header_get = CaptureFootBall._getDict4str(CaptureFootBall.HEADER_GET.format(user_agent))
        self.s = requests.session()

    @staticmethod
    def _getDict4str(strsource, match=':'):
        outdict = {}
        lists = strsource.split('\n')
        for list in lists:
            list = list.strip()
            if list:
                strbegin = list.find(match)
                outdict[list[:strbegin]] = list[strbegin+1:] if strbegin != len(list) else ''
        return outdict

    def _save_datas(self, mysql, good_datas, table, replace_columns):
        try:
            result_replace = True
            if not good_datas:
                return True
            if good_datas:
                operate_type = 'replace'
                result_replace = mysql.insert_batch(operate_type, table, replace_columns, good_datas)
            return result_replace
        except Exception, e:
            print '_save_datas error: {}.'.format(e)
            return False

    def analyze_data(self, url):
        res = self.s.get(url, headers=self.header_get)
        if res.status_code == 200:
            page_source = res.text.encode('utf-8')
        else:
            return False
        date = os.path.basename(url)[:-5]
        result_datas = []
        soup = BeautifulSoup(page_source, 'lxml')
        all_td = soup.find('div', {'class': 'leftList4'}).findAll('td')
        all_td = [all_td[i:i+9] for i in range(0, len(all_td), 9)]
        for x in all_td:
            resultData = {}
            resultData['round'] = x[0].getText().strip()
            resultData['time_begin'] = (date+x[1].getText().strip().encode('utf-8')+'00').replace(':','')
            resultData['team1'] = x[3].find('a').getText().strip().encode('utf-8')
            resultData['score'] = x[4].find('a').getText().replace('\n', '').replace(' ', '')
            resultData['team2'] = x[5].find('a').getText().strip().encode('utf-8')
            hash_str = resultData['time_begin'] + '-' + resultData['team1'] + '-' + resultData['team2']
            resultData['id'] = hashlib.md5(hash_str).hexdigest()
            result_datas.append(resultData)
        if len(result_datas) == 0:
            print 'page_source: {}'.format(page_source)
            raise ValueError('not get valid data')
        mysql = MysqldbOperate(DICT_MYSQL)
        table = CaptureFootBall.TABLE_NAME_REVIEW
        replace_columns = ['id', 'round', 'time_begin', 'score', 'team1', 'team2']
        self._save_datas(mysql, result_datas, table, replace_columns)
        del mysql
        return True

    @staticmethod
    def dateRange(beginDate, endDate):
        dates = []
        dt = datetime.strptime(beginDate, "%Y%m%d")
        date = beginDate
        while date <= endDate:
            dates.append(date)
            dt = dt + timedelta(1)
            date = dt.strftime("%Y%m%d")
        return dates

def main():
    startTime = datetime.now()
    useragent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
    objCaptureFootBall = CaptureFootBall(useragent)
    dates = objCaptureFootBall.dateRange('20180509', '20180510')
    url_format = 'http://goal.sports.163.com/schedule/{}.html'
    for date in dates:
        url = url_format.format(date)
        objCaptureFootBall.analyze_data(url)
        print 'end get date:{}'.format(date)
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()
