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
from sklearn.naive_bayes import MultinomialNB
from sklearn.externals import joblib
from nltk.stem.lancaster import LancasterStemmer
from nltk.corpus import stopwords
import  numpy as np
import nltk
import xlrd
import xlwt

import re
from os import listdir, remove
from os.path import isdir, join, exists
file_name = 'reviews'

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

#分词
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

def filter_emoji(desstr, restr=''):
    '''''
    过滤表情
    '''
    try:
        co = re.compile(u'[\U00010000-\U0010ffff]')
    except re.error:
        co = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]|\u263a\ufe0f|\ud83c\ude39|\ud83d\ude12|[\u2000-\u2764]|\u0060\u0060|\ufe0f|\u0026|\d+|invalid review')
    return co.sub(restr, desstr)

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
            # #英文单词词干化
            info = stemmint(info)
            check_info.append(info)
        except Exception, ex:
            logger.error('error info:{}, ex: {}'.format(info, ex))
            if add_null:
                check_info.append([])
    # all_stems = sum(check_info, [])
    # stems_once = set(stem for stem in set(all_stems) if all_stems.count(stem) == 1)
    # texts = [[stem for stem in text if stem not in stems_once] for text in check_info]
    try:
        texts = check_info
        return [' '.join(text) for text in texts]
        # return check_info
    except Exception:
        logger.error('text:{}'.format(info))

def check_null(a):
    if a[0]:
        return True

def generate_classifier_model(train_data, target, test_probability=0.0, target_names = {}):
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
    # print count_vect.get_feature_names()
    tfidf_transformer = TfidfTransformer()
    X_train_tfidf = tfidf_transformer.fit_transform(X_train_counts)
    # clf = MultinomialNB().fit(X_train_tfidf, y_train)
    from sklearn.linear_model import SGDClassifier
    clf = SGDClassifier(loss='hinge', penalty='l2', alpha=1e-3, max_iter=5, random_state=42).fit(X_train_tfidf, y_train)
    joblib.dump(clf, "clf")
    joblib.dump(count_vect, 'count_vect')
    if test_probability:
        X_new_counts = count_vect.transform(X_test)
        X_new_tfidf = tfidf_transformer.transform(X_new_counts)
        predicted = clf.predict(X_new_tfidf)
        logger.info('np.mean: {}'.format(np.mean(predicted == y_test)))


def load_classifier_model(test_data, target_name=None):
    clf = joblib.load('clf')
    count_vect = joblib.load('count_vect')
    old_test_data = test_data
    test_data = format_info(test_data, True)
    datas = filter(check_null, zip(test_data, old_test_data))
    datas = zip(*datas)
    test_data = datas[0]
    old_test_data = datas[1]
    X_new_counts = count_vect.transform(test_data)
    tfidf_transformer = TfidfTransformer()
    X_new_tfidf = tfidf_transformer.fit_transform(X_new_counts)
    predicted = clf.predict(X_new_tfidf)
    if target_name:
        for doc, category, c in zip(test_data, predicted, old_test_data):
            logger.info(("%r => %r => %s") % (c, doc, target_name[category]))
    else:
        for doc, category, c in zip(test_data, predicted, old_test_data):
            logger.info(("%r => %r => %s") % (c, doc, category))
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
            pattern = re.compile('"body":"[\s\S]*?","author":')
            bodys = pattern.findall(line)
            for body in bodys:
                body = body[8:-11]
                body = body.replace('\\n', '')
                reviews.append(body)
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

def get_record(app_name, begin_num=0, record_num=100000000000000):
    select_sql = 'select review from record_review where app_name="{}" order by review_time desc limit {},{};'.format(app_name, begin_num, record_num)
    record = mysql.sql_query(select_sql)
    record = [x[0] for x in record]
    return record

def save_excel(excel_name, sheet_name, test_data, predicted, target_name=None):
    try:
        if exists(excel_name):
            remove(excel_name)
        fd = xlwt.Workbook(encoding='utf-8')
        table = fd.add_sheet(sheet_name, cell_overwrite_ok=True)
        zip_data = zip(test_data, predicted)
        for i, data in enumerate(zip_data):
            if target_name:
                table.write(i, 0, data[0])
                table.write(i, 1, target_name[data[1]])
            else:
                table.write(i, 0, data[0])
                table.write(i, 1, data[1])
        fd.save(excel_name)
    except Exception, ex:
        logger.error('save_excel ex: {}'.format(ex))

# data,target,target_name = get_data_excel(r'C:\Users\Avazu Holding\Desktop\call flash v2.xlsx','Sheet1')
app_name = 'com.tencent.mm'
data, target, target_name = load_data(r'D:\capture_google_app\appbot')
generate_classifier_model(data, target, target_names=target_name, test_probability=0.3)
record = get_record(app_name)
test_data, predicted = load_classifier_model(record, target_name)
save_excel(r'C:\Users\Avazu Holding\Desktop\app.xls', app_name, test_data, predicted, target_name)
