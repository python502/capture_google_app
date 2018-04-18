#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/4/17 17:24
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : capture_google_review.py
# @Software: PyCharm
# @Desc    :
import json
import re
import time
import hashlib
import requests
import multiprocessing

from datetime import datetime
from retrying import retry

from logger import logger
from MysqldbOperate import MysqldbOperate

class PlayGoogleException(Exception):
    def __init__(self, err='play google error'):
        super(PlayGoogleException, self).__init__(err)


def retry_if_502_error(exception):
    return isinstance(exception, TypeError)

DICT_MYSQL = {'host': '127.0.0.1', 'user': 'root', 'passwd': '111111', 'db': 'capture', 'port': 3306}

class CaptureGoogleReview(object):
    HEADER_GET = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            user-agent:{}
            '''
    HEADER_POST = '''
            content-type:application/x-www-form-urlencoded;charset=UTF-8
            user-agent:{}
            '''
    id = '136880256'
    TABLE_NAME_REVIEW = 'record_review'
    def __init__(self, user_agent):
        self.header_get = CaptureGoogleReview._getDict4str(CaptureGoogleReview.HEADER_GET.format(user_agent))
        self.header_post = CaptureGoogleReview._getDict4str(CaptureGoogleReview.HEADER_POST.format(user_agent))
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
                # logger.info('_save_datas result_replace: {}'.format(result_replace))
            return result_replace
        except Exception, e:
            logger.error('_save_datas error: {}.'.format(e))
            return False

    @staticmethod
    def filter_emoji(desstr, restr=''):
        '''''
        过滤表情
        '''
        try:
            co = re.compile(u'[\U00010000-\U0010ffff]')
        except re.error:
            co = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]|\u263a\ufe0f|\ud83c\ude39')
        return co.sub(restr, desstr)

    def deal_first_reviews(self, app_name):
        url = 'https://play.google.com/store/apps/details?id={}&showAllReviews=true&hl=en'.format(app_name)
        res = self.s.get(url, headers=self.header_get)
        if res.status_code == 200:
            page_source = res.text.encode('utf-8')
        else:
            logger.error('url:{} get status error:{}'.format(url, res.status_code))
            raise PlayGoogleException('get status code error')

        pattern = re.compile(r'AF_initDataCallback\(\{key: [\s\S]+?;</script>', re.S)
        AF_initDataCallbacks = pattern.findall(page_source)
        for AF_initDataCallback in AF_initDataCallbacks:
            if AF_initDataCallback.find('[[["gp:') != -1:
                break
        else:
            logger.error('url:{} get first reviews form error')
            raise PlayGoogleException('get first reviews form error, not find gp:')
        AF_initDataCallback = AF_initDataCallback.split('data:function(){return')[1].split('}});</script>')[0]
        AF_initDataCallback = json.loads(AF_initDataCallback)
        if len(AF_initDataCallback) == 1:
            return (AF_initDataCallback[0], None)
        elif len(AF_initDataCallback) == 2:
            return (AF_initDataCallback[0], AF_initDataCallback[1][0])
        else:
            raise PlayGoogleException('len of AF_initDataCallback is {}'.format(len(AF_initDataCallback)))

    @retry(retry_on_exception=retry_if_502_error, stop_max_attempt_number=3, wait_fixed=2000)
    def deal_other_reviews(self, before_str):
        try:
            url = 'https://play.google.com/_/PlayStoreUi/data?hl=en'
            string_data = '[[[' + CaptureGoogleReview.id + ',[{"' + CaptureGoogleReview.id + '":[null,null,[2,null,[40,"' + before_str + '"]],["com.imangi.templerun2",7]]}],null,null,0]]]'
            res = self.s.post(
                url,
                data={'f.req': string_data},
                headers=self.header_post,
            )
            if res.status_code == 200:
                page_source = res.text.encode('utf-8')
            else:
                if res.status_code == 502:
                    raise TypeError('get status code 502 retry')
                else:
                    raise PlayGoogleException('get status code error')
            l = page_source.find('[["af.adr",')
            page_source = page_source[l:]
            page_source = json.loads(page_source)
            page_source = page_source[0][2][CaptureGoogleReview.id]
            if len(page_source) == 1:
                return (page_source[0], None)
            elif len(page_source) == 2:
                return (page_source[0], page_source[1][0])
            else:
                raise PlayGoogleException('len of page_source is {}'.format(len(page_source)))
        except Exception,e:
            logger.error('e:{}'.format(e))
            logger.error('deal_other_reviews get status error:{},before_str:{}'.format(res.status_code, before_str))
            raise

    def analyze_data(self, queue, app_name=''):
        t1 = 0
        first_flag = True
        mysql = MysqldbOperate(DICT_MYSQL)
        while 1:
            if queue.empty():
                if first_flag:
                    t1 = datetime.now()
                    first_flag = False
                else:
                    t2 = datetime.now()
                    if (t2 - t1).seconds > 7200:
                        logger.info('more than {}s not get datas'.format(7200))
                        break
                time.sleep(5)
            else:
                datas = queue.get()
                if not datas:
                    logger.info('analyze_data stop')
                    break
                st = datas[0]
                review_datas = datas[1]
                result_datas = self.__format_data(app_name, st, review_datas)
                table = CaptureGoogleReview.TABLE_NAME_REVIEW
                replace_columns = ['id', 'app_name', 'before_key', 'user_name', 'score', 'review', 'review_time', 'helpful']
                self._save_datas(mysql, result_datas, table, replace_columns)
                if len(review_datas) != 40:
                    logger.info('Get the last data')
                    break
                first_flag = True
                t1, t2 = 0, 0
        return True

    def __format_data(self, app_name, st, source_datas):
        results = []
        for data in source_datas:
            try:
                result = {'app_name': app_name, 'before_key': st}
                result['user_name'] = data[1][0].encode('utf-8').replace('\\','')
                result['score'] = data[2]
                review = data[4] or data[3]
                result['review'] = self.filter_emoji(review).encode('utf-8').replace('\\','')
                time_review = time.localtime(data[5][0])
                result['review_time'] = time.strftime('%Y%m%d%H%M%S', time_review)
                result['helpful'] = data[6]
                hash_str = app_name+'-'+result['user_name']+'-'+result['review_time']+'-'+result['review']
                result['id'] = hashlib.md5(hash_str).hexdigest()
            except Exception, e:
                logger.error('e:{}, data:{}'.format(e, data))
                continue
            results.append(result)
        return results

    def get_data(self, queue, app_name, query_conditions={}):
        before_id = query_conditions.get('before_id')
        begin_page = query_conditions.get('begin_page', 0)
        end_page = query_conditions.get('end_page', 10000000)
        now_page = 1
        if before_id:
            st = before_id
        else:
            result, st = self.deal_first_reviews(app_name)
            if now_page>=begin_page and now_page<=end_page:
                queue.put([None, result])

        while st:
            st_src = st
            result, st = self.deal_other_reviews(st)
            now_page += 1
            logger.info('now_page: {}'.format(now_page))
            if now_page>=begin_page and now_page<=end_page:
                queue.put([st_src, result])
            elif now_page>end_page:
                queue.put(None)
                logger.info('')
                return True
            else:
                continue
        else:
            queue.put(None)
            return True

    def deal_main(self, app_name):
        manager = multiprocessing.Manager()
        # queue = manager.Queue(maxsize = 1000)
        queue = manager.Queue()
        query_conditions = {'begin_page': 0,'end_page': 15}
        p1 = multiprocessing.Process(target=self.get_data, args=(queue, app_name, query_conditions,))
        p2 = multiprocessing.Process(target=self.analyze_data, args=(queue, app_name,))

        p1.start()
        p2.start()

        p1.join()
        p2.join()

def main():
    startTime = datetime.now()
    useragent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
    # app_name = 'com.imangi.templerun2'
    app_name = 'com.orangeapps.piratetreasure'
    objCaptureGoogleReview = CaptureGoogleReview(useragent)
    # results = []
    # result, st = objCaptureGoogleReview.deal_first_reviews(app_name)
    # while st:
    #     result, st = objCaptureGoogleReview.deal_other_reviews(st)
    #     print result[0]
    #     results.extend(result)
    #     print len(results)
    # print results
    objCaptureGoogleReview.deal_main(app_name)
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()
