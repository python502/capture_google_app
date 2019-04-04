#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/4/17 11:58
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : test.py
# @Software: PyCharm
# @Desc    :
import random
import numpy as np
from logger import logger

# random.shuffle(range,begin,end)
for n in range(1,60):
    all = []
    for i in range(5):
        begin = i*15+1
        end = begin+14
        columns = []
        while len(columns)!=5:
            value = random.randint(begin,end)
            if value in columns:
                continue
            columns.append(value)
        all.append(columns)
    all[2][2] = 'Free'
    result = map(list, zip(*all))
    logger.info('Sequence: {}'.format(n))
    logger.info('Card: {}'.format(result))








