#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/4/24 10:32
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : rm all file pyc
# @Software: PyCharm
# @Desc    :
import requests
from urllib import urlencode
from bs4 import BeautifulSoup
s = requests.session()

#Cookie 改成自己的cookie
HEADER_GET = '''
        Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
        Accept-Encoding:gzip, deflate
Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
Cache-Control:no-cache
Connection:keep-alive
Content-Type:application/x-www-form-urlencoded

Origin:sandglass.avazudata.com
Pragma:no-cache
Upgrade-Insecure-Requests: 1
        User-Agent:{}
        '''

def getDict4str(strsource, match=':'):
    outdict = {}
    lists = strsource.split('\n')
    for list in lists:
        if list.startswith('#'):
            continue
        list = list.strip()
        if list:
            strbegin = list.find(match)
            outdict[list[:strbegin].strip()] = list[strbegin+1:].strip() if strbegin != len(list) else ''
    return outdict

#本机user_agent
user_agent='Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.79 Safari/537.36'
header_get = getDict4str(HEADER_GET.format(user_agent))

def stop_task(id):
    url = 'http://sandglass.avazudata.com/instance_page/stop_submit?instance_id={}'.format(id)
    res = s.get(url, headers=header_get)
    print '{} stop {}'.format(id, res.status_code)
# stop_task('3653785')
def rerun_task(data):
    url = 'http://sandglass.avazudata.com/instance_page/run_submit'
    res = s.post(url, headers=header_get, data=data)
    print '{} rerun {}'.format(data['pt'], res.status_code)


def get_instance_id(parameter):
    url = 'http://sandglass.avazudata.com/instance_page?{}'
    para = urlencode(parameter)
    url = url.format(para)
    res = s.get(url, headers=header_get)
    print 'task_id:{} get info {}'.format(parameter.get('task_id'), res.status_code)
    if res.status_code == 200:
        page_source = res.text.encode('utf-8')
    else:
        return
    soup = BeautifulSoup(page_source, 'lxml')
    all_taskinfos = soup.find('table', {'class': 'table table-bordered table-hover'}).find('tbody').findAll('tr')
    instance_ids = []
    for taskinfo in all_taskinfos:
        # pt  = taskinfo.findAll('td')[4].getText().strip()
        # if not pt.startswith('2018-12-11'):
        #     continue
        if taskinfo.findAll('td')[6].getText().strip()== 'done':
            continue
        instance_id = taskinfo.findAll('td')[1].getText().strip()
        instance_ids.append(int(instance_id))
    print 'instance_ids:{}'.format(instance_ids)
    return instance_ids

def main_get(task_id):
    parameter = {'id': '',
                 'task_id': task_id,
                 'pt': '',
                 'inittime': '',
                 'worker': '',
                 'state': 'waiting,suspending,running'}
    return get_instance_id(parameter)



def main_get_instance(ins_id):
    parameter = {'id': ins_id,
                 'task_id': '',
                 'pt': '',
                 'inittime': '',
                 'worker': '',
                 'state': ''}
    return get_instance_info(parameter)

def get_instance_info(parameter):
    url = 'http://sandglass.avazudata.com/instance_page?{}'
    para = urlencode(parameter)
    url = url.format(para)
    res = s.get(url, headers=header_get)
    print 'ins_id:{} get info {}'.format(parameter.get('id'), res.status_code)
    if res.status_code == 200:
        page_source = res.text.encode('utf-8')
    else:
        return
    soup = BeautifulSoup(page_source, 'lxml')
    all_taskinfos = soup.find('table', {'class': 'table table-bordered table-hover'}).find('tbody').findAll('tr')
    task_infos = []
    for taskinfo in all_taskinfos:
        tmp={}
        if taskinfo.findAll('td')[6].getText().strip()== 'done':
            continue
        tmp['task_id'] = taskinfo.findAll('td')[2].getText().strip()
        tmp['pt'] = taskinfo.findAll('td')[4].getText().strip()
        task_infos.append(tmp)
    print 'task_infos:{}'.format(task_infos)
    return task_infos
#不循环的话，此处放入id列表
def main_stop():
    # for i in ('1142'):
    id_list = main_get(1164)
    # print id_list
    # id_list = [532925,532926,532930,532936,532937,532942,532943,532948,532955,533173,533175,533247,533458,533460,534569,537584]
    for i in id_list:
        # print i
        stop_task(i)


def main_rerun():
    ids=[530]
    for id in ids:
        head_s = '2019-03-25 '
        hour_s = [str(x).zfill(2) for x in range(1,24)]
        pts = []
        # for day in day_s:
        #     pts = []
        #     head_d = head_s+day+' '
        for hour in hour_s:
            head_h = head_s+hour+':00'
            pts.append(head_h)
        pt = str(pts)[1:-1].replace('\'', '')
        # for pt in pts:
        data = {}
        data['task_id'] = id
        data['run_mode'] = 'renew'
        data['max_chain'] = '0'
        data['pt'] = pt
        rerun_task(data)
    # pts = ['2018-10-10 23:00','2018-10-11 23:00','2018-10-12 23:00','2018-10-13 23:00','2018-10-14 23:00','2018-10-15 23:00','2018-10-16 23:00','2018-10-17 23:00','2018-10-18 23:00','2018-10-19 23:00','2018-10-20 23:00','2018-10-21 23:00','2018-10-22 23:00']
    # for pt in pts:
    #     data = {}
    #     data['task_id'] = 1142
    #     data['run_mode'] = 'rerun'
    #     data['max_chain'] = 'unlimited'
    #     data['pt'] = pt

    # id_list = '532930, 532936, 532937, 532942, 532943, 532948, 532955, 533173, 533175, 533247, 533458,533460, 534569, 537584'
    # infos = main_get_instance(id_list)
    # for info in infos:
    #     data = {}
    #     data['task_id'] = info.get('task_id')
    #     data['run_mode'] = 'renew'
    #     data['max_chain'] = 'unlimited'
    #     data['pt'] = info.get('pt')
    #     rerun_task(data)
    # ids = [19]
    # for id in ids:
    #     data = {}
    #     data['task_id'] = id
    #     data['run_mode'] = 'renew'
    #     data['max_chain'] = 'unlimited'
    #     data['pt'] = '>=2018-12-18 00:00'
    #     rerun_task(data)

def  get_task_id():
    pass

def set_done(instance_id):
    url = 'http://sandglass.avazudata.com/instance_page/set_done?instance_id={}'.format(instance_id)
    res = s.get(url, headers=header_get)
    print '{} set_done {}'.format(instance_id, res.status_code)

def main_setdone():
    parameter = {'id': '',
                 'task_id': '1163',
                 'pt': '',
                 'inittime': '>=-7days',
                 'worker': '',
                 'state': 'failed'}
    ids = get_instance_id(parameter)
    # ids = [4686498,4686499,4686500,4686501,4686502,4686503,4686504,4686505,4686506,4686507,4686508]
    for id in ids:
        set_done(id)


def get_task_status(parameter):
    url = 'http://sandglass.avazudata.com/task_page?{}'
    para = urlencode(parameter)
    url = url.format(para)
    res = s.get(url, headers=header_get)
    print 'task_id:{} get info {}'.format(parameter.get('task_id'), res.status_code)
    if res.status_code == 200:
        page_source = res.text.encode('utf-8')
    else:

        return

def set_state_on(id):
    #http://195.201.104.81:8000/task_page/enable_submit?id=265
    url = 'http://195.201.104.81:8000/task_page/enable_submit?{}'
    para = urlencode(parameter)
    url = url.format(para)
    res = s.get(url, headers=header_get)
    print 'task_id:{} stateon {}'.format(id, res.status_code)
    if res.status_code == 200:
        page_source = res.text.encode('utf-8')
    else:

        return
if __name__ == "__main__":
    # parameter={'idname': '365',
    # 'worker': '',
    # 'enable': '0,1'}
    # ids = ['1115','265','266','267','268','269','270','287','288','291','308','310','311','321','322','323','274','272','273','275','1116']
    # for id_i in ids:
    #     parameter={'id':id_i}
    #     set_state_on(parameter)
    main_setdone()
    # main_rerun()
    # main_stop()



