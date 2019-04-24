# -*- coding: utf-8 -*-
# Created by sunze  on 2018/9/4
"""
为了给演示提升计算速度，使用均值加上以一个以实际销量方差波动的方式作为预测值
@author: sun ze
"""
import numpy as np
import random
from random import seed
from pandas import Series


# 使用前90天的销量加上改时段的方差的波动量作为未来的预测销量
def simple_predict(sale_list):

    sigma = np.std(sale_list)

    # pre_list = sale_list[:days_to_pre].apply(lambda x:x+random.gauss( 0, sigma))
    seed(10)
    pre_list = sale_list.apply(lambda x: int(abs(x+random.gauss(0, 0.6*sigma))))

    return pre_list

def get_accuracy(sale_list,predicted_list):

    sum_sale = []
    for (i, j) in zip(sale_list,predicted_list):
        if i > 0:
            sum_sale.append(abs(j-i)/i)
    return round(np.mean(sum_sale),2)



if __name__ == '__main__':

    sale_list = list(np.random.randint(80,100, size=90))
    predicted_list = simple_predict(Series(sale_list))
    print(predicted_list)
    print(get_accuracy(sale_list,predicted_list))