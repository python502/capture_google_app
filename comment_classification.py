#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/4/24 10:32
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : comment_classification.py
# @Software: PyCharm
# @Desc    :

from MysqldbOperate import MysqldbOperate
DICT_MYSQL = {'host': '127.0.0.1', 'user': 'root', 'passwd': '111111', 'db': 'capture', 'port': 3306}

mysql = MysqldbOperate(DICT_MYSQL)
select_sql = 'select review from {} where app_name="{}"'.format('record_review', 'com.mobdub.channel.KUAM')
record = mysql.sql_query(select_sql)
record = [x[0] for x in record]
record = record[:2000]


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
import re
import xlrd

#去停用词
def stopword(words):
    try:
        english_stopwords = stopwords.words('english')
        words = [word for word in words if word not in english_stopwords]
        english_punctuations = ['\'s', '...', ',', '.', ':', ';', '?', '(', ')', '[', ']', '&', '!', '*', '@', '#', '$',\
                                '%', 'invalid review', 'n\'t']
        words = [word for word in words if word not in english_punctuations]
        return words
    except Exception:
        print 'error:words {}'.format(words)
        raise

#分词
def deal_review(record):
    words = []
    sens = nltk.sent_tokenize(record)
    for sen in sens:
        word = nltk.word_tokenize(sen)
        words.extend(word)
    return words

def filter_emoji(desstr, restr=''):
    '''''
    过滤表情
    '''
    try:
        co = re.compile(u'[\U00010000-\U0010ffff]')
    except re.error:
        # co = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]|\u263a\ufe0f|\ud83c\ude39|\ud83d\ude12|\u2026|\u2764|\u2661|\u263a|\u200b|\u2665')
        co = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]|\u263a\ufe0f|\ud83c\ude39|\ud83d\ude12|[\u2000-\u2764]')
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
            info = filter_emoji(info)
            if isinstance(info, unicode):
                info = info.encode('utf-8')
            info = deal_review(info)
            info = stopword(info)
            #英文单词词干化
            info = stemmint(info)
            check_info.append(info)
        except Exception:
            print 'error info:{}'.format(info)
            if add_null:
                check_info.append([])
    all_stems = sum(check_info, [])
    stems_once = set(stem for stem in set(all_stems) if all_stems.count(stem) == 1)
    texts = [[stem for stem in text if stem not in stems_once] for text in check_info]
    return [' '.join(text) for text in texts]

def check_null(a):
    if a[0]:
        return True

def generate_classifier_model(train_data, target, test_probability=0.0, target_names = []):
    train_data = format_info(train_data)
    datas = filter(check_null, zip(train_data, target))
    datas = zip(*datas)
    train_data = datas[0]
    target = datas[1]
    train = Bunch(data=train_data, filenames=[], target_names=target_names, target=target, DESCR=[])
    X_train, X_test, y_train, y_test = model_selection.train_test_split(
        train.data, train.target, test_size=test_probability)
    count_vect = CountVectorizer(stop_words="english",decode_error='ignore')
    X_train_counts = count_vect.fit_transform(X_train)
    tfidf_transformer = TfidfTransformer()
    X_train_tfidf = tfidf_transformer.fit_transform(X_train_counts)
    clf = MultinomialNB().fit(X_train_tfidf,y_train)
    joblib.dump(clf, "clf")
    joblib.dump(count_vect, 'count_vect')
    if test_probability:
        X_new_counts = count_vect.transform(X_test)
        X_new_tfidf = tfidf_transformer.transform(X_new_counts)
        predicted = clf.predict(X_new_tfidf)
        print np.mean(predicted == y_test)


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
            print("%r => %r => %s") % (c, doc, target_name[category])



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
#
data,target,target_name = get_data_excel(r'C:\Users\Avazu Holding\Desktop\call flash .xlsx','Sheet1')

generate_classifier_model(data, target, test_probability=0.3)
load_classifier_model(record[:-1000], target_name)

