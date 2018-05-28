#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/4/24 10:32
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : comment_classification.py
# @Software: PyCharm
# @Desc    :
from logger import logger
from MysqldbOperate import MysqldbOperate
DICT_MYSQL = {'host': '127.0.0.1', 'user': 'root', 'passwd': '111111', 'db': 'capture', 'port': 3306}

mysql = MysqldbOperate(DICT_MYSQL)

from sklearn.utils import Bunch
from sklearn import model_selection
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.externals import joblib
from nltk.stem.lancaster import LancasterStemmer
from sklearn.linear_model import SGDClassifier
from nltk.corpus import stopwords
from sklearn.naive_bayes import MultinomialNB
from os import listdir, remove
from os.path import isdir, join, exists
from xlutils.copy import copy
from decimal import Decimal

import  numpy as np
import pandas as pd
import nltk
import xlrd
import xlwt
import json
import re
import os
import datetime


file_name = 'reviews'
file_rename = 'reviews_bak'
file_other = 'reviews_other'
result_xls = r'D:\capture_google_app\results\review_classification_{}.xls'
from itertools import izip
# topic2id = {'Satisfied users': 1,
#             'Security & Accounts': 2,
#             'Bugs': 3,
#             'Design & UX': 4,
#             'Dissatisfied users': 5
#             }
# id2topic = dict(izip(topic2id.itervalues(), topic2id.iterkeys()))

#去停用词
def stopword(words):
    try:
        # english_stopwords = stopwords.words('english')
        # words = [word for word in words if word not in english_stopwords]
        # english_punctuations = ['\'s', '...', ',', '.', ':', ';', '?', '(', ')', '[', ']', '&', '!', '*', '@', '#', '$',\
        #                         '%', 'n\'t']
        english_punctuations = ['\'s', '...', ',', '.', ':', ';', '?', '(', ')', '[', ']', '&', '!', '*', '@', '#', '$',\
                                '%']
        words = [word for word in words if word not in english_punctuations]
        return words
    except Exception:
        logger.error('error:words {}'.format(words))
        raise

#将评论语句分词
def deal_review(record):
    try:
        words = []
        sens = nltk.sent_tokenize(record)
        for sen in sens:
            word = nltk.word_tokenize(sen)
            words.extend(word)
        return words
    except Exception, ex:
        logger.error('error record: {},ex: {}'.format(record, ex))
        raise

#过滤掉表情
def filter_emoji(desstr, restr=''):
    try:
        co = re.compile(u'[\U00010000-\U0010ffff]')
    except re.error:
        co = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]|\u263a\ufe0f|\ud83c\ude39|\ud83d\ude12|[\u2000-\u2764]|\u0060\u0060|\ufe0f|\u0026|\d+|invalid review')
    return co.sub(restr, desstr)

#词干化
def stemmint(words):
    st = LancasterStemmer()
    results = []
    for word in words:
        word = st.stem(word)
        results.append(word)
    return results

def format_info(infos, add_null=False):
    check_info = []
    for info in infos:
        try:
            info = info.lower()
            if not isinstance(info, unicode):
                info = info.decode('utf-8')
            info = filter_emoji(info)
            info = deal_review(info)
            info = stopword(info)
            info = stemmint(info)
            check_info.append(info)
        except Exception, ex:
            logger.error('error info:{}, ex: {}'.format(info, ex))
            if add_null:
                check_info.append([])

    # 去掉仅出现一次的词语
    all_stems = sum(check_info, [])
    stems_once = set(stem for stem in set(all_stems) if all_stems.count(stem) == 1)
    texts = [[stem for stem in text if stem not in stems_once] for text in check_info]
    try:
        # texts = check_info
        return [' '.join(text) for text in texts]
    except Exception:
        logger.error('text:{}'.format(info))

#去空数据
def check_null(a):
    if a[0]:
        return True

def get_best_parameters(clf, parameters,X,y):
    from sklearn.model_selection import GridSearchCV
    gs_clf = GridSearchCV(clf, parameters, n_jobs=-1)
    gs_clf = gs_clf.fit(X, y)
    best_parameters, score = gs_clf.best_params_, gs_clf.best_score_
    for param_name in sorted(parameters.keys()):
        logger.info("%s: %r" % (param_name, best_parameters[param_name]))
    logger.info('best score %s' % (score,))
    return best_parameters

#生成分类训练模型
def generate_classifier_model(train_data, target, test_probability=0.0, target_names = {},allow_pass=0.8):
    train_data = format_info(train_data)
    datas = filter(check_null, zip(train_data, target))
    datas = zip(*datas)
    train_data = datas[0]
    target = datas[1]
    train = Bunch(data=train_data, filenames=[], target_names=list(target_names.itervalues()), target=target, DESCR=[])
    X_train, X_test, y_train, y_test = model_selection.train_test_split(
        train.data, train.target, test_size=test_probability)
    count_vect = CountVectorizer(decode_error='ignore')
    X_train_counts = count_vect.fit_transform(X_train)
    # logger.info('feature name:{}'.format(count_vect.get_feature_names()))
    tfidf_transformer = TfidfTransformer()
    X_train_tfidf = tfidf_transformer.fit_transform(X_train_counts)
    # clf = MultinomialNB().fit(X_train_tfidf, y_train)

    # parameters = {
    #     'alpha': (1e-2, 1e-4)
    # }
    # clf = SGDClassifier(loss='hinge', penalty='l2', max_iter=5, random_state=42)
    # best_parameters = get_best_parameters(clf, parameters, X_train_tfidf, y_train)

    clf = SGDClassifier(loss='hinge', penalty='l2', alpha=1e-4, max_iter=5, random_state=42).fit(X_train_tfidf, y_train)
    if os.path.exists('./clf'):
        os.remove('./clf')
    if os.path.exists('./count_vect'):
        os.remove('./count_vect')
    # print clf.score(X_train_tfidf, y_train)
    joblib.dump(clf, "clf")
    joblib.dump(count_vect, 'count_vect')
    if test_probability:
        X_new_counts = count_vect.transform(X_test)
        X_new_tfidf = tfidf_transformer.transform(X_new_counts)
        predicted = clf.predict(X_new_tfidf)
        mean = np.mean(predicted == y_test)
        logger.info('np.mean: {}'.format(mean))
        # for l, m, n in zip(X_test, y_test, predicted):
        #     if m != n:
        #         logger.info(("%r => %s => %s") % (l, target_names[m], target_names[n]))
        if mean >= allow_pass:
            return True
        else:
            return False
    return True

#导入分类模型 进行数据分类
def load_classifier_model(test_data, target_name=None):
    clf = joblib.load('clf')
    count_vect = joblib.load('count_vect')
    old_test_data = test_data
    test_data = [x[0] for x in test_data]
    test_data = format_info(test_data, True)
    datas = filter(check_null, zip(test_data, old_test_data))
    datas = zip(*datas)
    test_data = datas[0]
    old_test_data = datas[1]
    if not test_data:
        raise ValueError('after format test_data is null')
    X_new_counts = count_vect.transform(test_data)
    tfidf_transformer = TfidfTransformer()
    X_new_tfidf = tfidf_transformer.fit_transform(X_new_counts)
    predicted = clf.predict(X_new_tfidf)
    if target_name:
        for doc, category, c in zip(test_data, predicted, old_test_data):
            logger.debug(("%r => %r => %s") % (c[0], doc, target_name[category]))
    else:
        for doc, category, c in zip(test_data, predicted, old_test_data):
            logger.debug(("%r => %r => %s") % (c[0], doc, category))
    return old_test_data, predicted

def get_data_excel(excel_name, sheet_name):
    data = xlrd.open_workbook(excel_name)
    table = data.sheet_by_name(sheet_name)
    review = 1
    instructions = 2
    data_review = []
    data_instructions = []
    for i in range(1, table.nrows):
        data_review.append(table.cell(i, review).value)
        data_instructions.append(table.cell(i, instructions).value)

    ins = list(set(data_instructions))
    set_ins = {}
    return_ins = {}
    for i, element in enumerate(ins):
        set_ins[element] = i
        return_ins[i] = element
    target = []
    for data in data_instructions:
        target.append(set_ins[data])
    return data_review, target, return_ins

def get_local_reviews(base_path, data_file_name):
    reviews = []
    with open(join(base_path, data_file_name, file_name)) as fd:
        lines = fd.readlines()
        for line in lines:
            line = line.replace('\\n', '')
            reviews.append(line)
    return reviews

def load_data(base_path, categories = None):
    target = []
    target_names = {}
    datas = []
    folders = [f for f in sorted(listdir(base_path))
               if isdir(join(base_path, f))]

    if categories is not None:
        folders = [f for f in folders if f in categories]

    for i, folder in enumerate(folders):
        target_names[i] = folder
        data = get_local_reviews(base_path, folder)
        datas.extend(data)
        target.extend(len(data) * [i])
    return datas, target, target_names


def rm_duplicate(scr_datas):
    if not scr_datas:
        return scr_datas
    data = pd.DataFrame(scr_datas)
    # 去重
    data = data.drop_duplicates()
    return data.to_dict(orient='list').get(0)

    #resave_type
    #0 only reviews_bak
    #1 only reviews_other
    #2 all
def resave_data(base_path, resave_type=1):
    if resave_type in [0, 2]:
        mode = 'w'
    else:
        mode = 'a'

    folders = [f for f in sorted(listdir(base_path))
               if isdir(join(base_path, f))]
    for folder in folders:
        other_file = join(base_path, folder, file_other)
        with open(join(base_path, folder, file_name), mode=mode) as fd:
            if resave_type == 0 or resave_type == 2:
                data = get_appbot_reviews(base_path, folder)
                data = rm_duplicate(data)
                for d in data:
                    try:
                        # if not isinstance(d,unicode):
                        #     d = d.encode('utf-8')
                        d = filter_emoji(d)
                        fd.write(d.strip())
                        fd.write('\n')
                    except UnicodeEncodeError:
                        logger.error('folder:{} write {}'.format(folder, d.encode('utf-8')))


            if (resave_type == 1 or resave_type == 2) and os.path.exists(other_file):
                with open(other_file, mode='r') as rd:
                    lines = rd.readlines()
                    if not lines: continue
                    lines = rm_duplicate(lines)
                    for line in lines:
                        fd.write(line)

        


# def get_appbot_reviews(base_path, data_file_name):
#     reviews = []
#     with open(join(base_path, data_file_name, file_rename)) as fd:
#         lines = fd.readlines()
#         for line in lines:
#             pattern = re.compile('"body":"[\s\S]*?","author":')
#             bodys = pattern.findall(line)
#             for body in bodys:
#                 body = body[8:-11]
#                 body = body.replace('\\n', '')
#                 reviews.append(body)
#     return reviews


def get_appbot_reviews(base_path, data_file_name):
    reviews = []
    file_path = join(base_path, data_file_name, file_rename)
    if not os.path.exists(file_path):
        return []
    with open(file_path) as fd:
        lines = fd.readlines()
        for line in lines:
            if line == '\n':continue
            info = json.loads(line)
            info = info.get('reviews')
            for review in info:
                topic_ids = review.get('topic_ids')
                if 1 != len(topic_ids):
                    continue
                # author = review.get('author')
                # if not author:
                #     continue
                body = review.get('body').replace('\n', '').strip()
                reviews.append(body)
    return reviews

def get_record(app_name, delta=0, begin_num=0, record_num=100000000000000):
    if delta:
        now = datetime.datetime.now()
        delta = datetime.timedelta(days=-delta)
        delta_time = now + delta
        delta_time = delta_time.strftime('%Y%m%d')
        select_sql = 'select review,score,review_time,helpful from record_review where app_name="{}" and review_time >"{}" order by review_time desc limit {},{};'.format(app_name, delta_time, begin_num, record_num)
    else:
        select_sql = 'select review,score,review_time,helpful from record_review where app_name="{}" order by review_time desc limit {},{};'.format(
            app_name, begin_num, record_num)

    record = mysql.sql_query(select_sql)
    record = [x for x in record]
    return record

def save_excel(excel_name, sheet_name, test_data, predicted, target_name=None):
    try:
        #xlwt.Workbook是创建一个新的空excel
        if exists(excel_name):
            rd = xlrd.open_workbook(excel_name, formatting_info=True)
            fd = copy(rd)
        else:
            fd = xlwt.Workbook(encoding='utf-8')

        all_data = []
        for x, y in zip(test_data, predicted):
            x = list(x)
            x.append(y)
            all_data.append(x)
        records = get_count_mean(all_data)

        table = fd.add_sheet(sheet_name, cell_overwrite_ok=True)
        title = ['Topic', 'HighCounts', 'HighScore', 'LowCounts', 'LowScore']
        for i, data in enumerate(title):
            table.write(0, i, data)
        for i, data in enumerate(records, 1):
            if target_name:
                table.write(i, 0, target_name[data.get('target')])
            else:
                table.write(i, 0, data.get('target'))
            table.write(i, 1, data.get('big_count'))
            table.write(i, 2, data.get('big_mean'))
            table.write(i, 3, data.get('small_count'))
            table.write(i, 4, data.get('small_mean'))
        row = i+3
        title = ['Topic', 'ReviewTime', 'Score', 'Like', 'Length', 'Review']
        for i, data in enumerate(title):
            table.write(row, i, data)
        for i, data in enumerate(all_data, row+1):
            if target_name:
                table.write(i, 0, target_name[data[4]])
            else:
                table.write(i, 0, data[4])
            table.write(i, 1, data[2].strftime("%Y%m%d%H%M%S"))
            table.write(i, 2, data[1])
            table.write(i, 3, data[3])
            table.write(i, 4, len(data[0]))
            table.write(i, 5, data[0])
        fd.save(excel_name)
    except Exception, ex:
        logger.error('save_excel ex: {}'.format(ex))

def get_count_mean(data):
    columns = ['review', 'score', 'review_time', 'like', 'target_name']
    d = pd.DataFrame(data, columns=columns)
    grouped = d.groupby('target_name')
    records = []
    for g in grouped:
        record = {}
        record['target'] = g[0]
        g_data_big = g[1].ix[g[1]['score'] >= 4]
        g_data_big = dict(g_data_big.score.agg(['count', 'mean']))
        g_data_small = g[1].ix[g[1]['score'] < 4]
        g_data_small = dict(g_data_small.score.agg(['count', 'mean']))

        record['big_count'] = int(g_data_big.get('count'))
        record['big_mean'] = Decimal(str(g_data_big.get('mean'))).quantize(Decimal('0.00')) if record['big_count'] else 0.0
        record['small_count'] = int(g_data_small.get('count'))
        record['small_mean'] = Decimal(str(g_data_small.get('mean'))).quantize(Decimal('0.00')) if record['small_count'] else 0.0
        records.append(record)
    return records

#从文件夹中提取训练集
def main():
    app_name = 'com.appconnect.easycall'
    data, target, target_name = load_data(r'D:\capture_google_app\appbot')
    generate_classifier_model(data, target, target_names=target_name, test_probability=0.3)
    # record = get_record(app_name, 7)
    # test_data, predicted = load_classifier_model(record, target_name)
    # save_excel(result_xls, app_name, test_data, predicted, target_name)

#从xlsx中提取训练集数据
def main1():
    app_name = 'com.appconnect.easycall'
    data, target, target_name = get_data_excel(r'C:\Users\Avazu Holding\Desktop\call flash v2.xlsx', 'Sheet1')
    generate_classifier_model(data, target, target_names=target_name, test_probability=0.3)
    record = get_record(app_name, 7)
    test_data, predicted = load_classifier_model(record, target_name)
    save_excel(result_xls, app_name, test_data, predicted, target_name)

#重新格式化数据  将从appbot上复制的数据 提取出来
def main2():
    resave_data(r'D:\capture_google_app\appbot')
def main3():
    data = get_record('com.appconnect.easycall', 7)
    print type(data[0][2])
if __name__ == '__main__':
    #knn 0.58
    #svm 0.25
    #SGD 0.63
    #MultinomialNB 0.56
    main()
