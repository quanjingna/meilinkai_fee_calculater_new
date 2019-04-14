# -*- coding: utf-8 -*-
# Created by todoit on 2018/4/11

import numpy as np
import pandas as pd
import simple_predict
from scipy.stats import norm
import math
from sklearn.metrics import r2_score


# 连续性检查补货策略（R,Q）计算，定量不定期
def QRStockPoint(sale_mean, sale_std, transportday_mean, transportday_var,
                        eoq, k):
    """
    :param sale_mean: mean of predict daily sale
    :param sale_std <float>: the Standard Deviation of sale
    :param transportday_mean:  mean of transport day
    :param transportday_var: variation of transport day
    :param  eoq:  Economic Order Quantity
    :param  k: service level(value 0 to 1)

    :return: safe stock point, import stock point, expect stock point

    """
    safe_stock = norm.ppf(k) * math.sqrt(pow(sale_mean, 2) * transportday_var + transportday_mean *pow(sale_std, 2))
    import_stock = transportday_mean*sale_mean+safe_stock
    expect_stock = import_stock + eoq
    return safe_stock, import_stock, expect_stock


# 周期性检查补货策略（T,s）计算，定期不定量
def TsStockPoint(sale_mean, sale_std, transportday_mean, transportday_var,
                 review_period, k):
    """
    :param sale_mean: mean of predict daily sale
    :param sale_std <float>: the Standard Deviation of sale
    :param transportday_mean:  mean of transport day
    :param transportday_var: variation of transport day
    :param review_period: time period to review the inventory
    :param  k: service level (value 0 to 1)
    :return: safe stock point, expect stock point
    """
    safe_stock =norm.ppf(k) * math.sqrt(pow(sale_mean, 2) * transportday_var + (transportday_mean+review_period) *pow(sale_std, 2) )
    # import_stock = transportday_mean*sale_mean+safe_stock
    expect_stock = ((transportday_mean+review_period))*sale_mean+safe_stock
    return safe_stock, expect_stock



# 周期性检查补货策略（S,s）计算，(R,Q)(T,S)模型的结合
def SsStockPoint(sale_mean, sale_std, transportday_mean, transportday_var,
                 review_period, k):
    """
    :param sale_mean: mean of predict daily sale
    :param sale_std <float>: the Standard Deviation
    :param transportday_mean:  mean of transport day
    :param transportday_var: variation of transport day
    :param review_period: time period to review the inventory
    :param k: service level (value 0 to 1)
    :return: safe stock point, import stock point, expect stock point
    """
    safe_stock = norm.ppf(k) * math.sqrt(pow(sale_mean, 2) * transportday_var + transportday_mean * pow(sale_std, 2))
    import_stock = transportday_mean*sale_mean+ safe_stock
    expect_stock = (transportday_mean+review_period)*sale_mean+math.sqrt(pow(sale_mean, 2) * transportday_var + (transportday_mean+review_period) * pow(sale_std, 2))
    return safe_stock, import_stock, expect_stock

def pro_SsStockPoint(sale_mean, sale_std, transportday_mean, transportday_var,
                 review_period, k, nrtk =1):
    """
    :param sale_mean: mean of predict daily sale
    :param sale_std <float>: the Standard Deviation
    :param transportday_mean:  mean of transport day
    :param transportday_var: variation of transport day
    :param review_period: time period to review the inventory
    :param k: service level (value 0 to 1)
    :return: safe stock point, import stock point, expect stock point
    """
    safe_stock = norm.ppf(k) * math.sqrt(pow(sale_mean, 2) * transportday_var + (transportday_mean+review_period) * pow(sale_std, 2))
    import_stock = transportday_mean*sale_mean+ safe_stock + review_period*sale_mean *nrtk
    expect_stock = transportday_mean*sale_mean+ safe_stock + review_period*sale_mean
    return safe_stock, import_stock, expect_stock

def stock_simulation(sale_serie,replenish_period,transportday_mean,k,ini_stock=None,min_replesh=0):


    sale_std = np.std(sale_serie)
    # pre_list = sale_list[:days_to_pre].apply(lambda x:x+random.gauss( 0, sigma))
    sale_index = sale_serie.index
    if ini_stock is None:
        ini_stock = sum(sale_serie[sale_index[0]:sale_index[replenish_period+transportday_mean]])
    stock_seri = pd.Series(index=sale_index)
    replenish_seri = pd.Series(index=sale_index)
    stock_seri[sale_index[0]] = ini_stock
    sale_mean = np.mean(sale_serie[sale_index[0]:sale_index[replenish_period]])
    _ ,import_stock,expect_stock = SsStockPoint(sale_mean, sale_std, transportday_mean, 0,
                                    replenish_period, k)
    stock = ini_stock
    expect_stock = int(expect_stock)
    import_stock = int(import_stock)
    if (stock < import_stock) & ((expect_stock - stock) >= min_replesh):
        replenish_seri[sale_index[0]] = int(expect_stock - stock)
    else:
        replenish_seri[sale_index[0]] = 0

    for i in np.arange(1,len(sale_index),1):

        last_day = (len(sale_index)-max(replenish_period,transportday_mean))
        if i <= last_day:
            sale_mean = np.mean(sale_serie[sale_index[i]:sale_index[i + replenish_period-1]])
        else:
            sale_mean = np.mean(sale_serie[sale_index[last_day]:])

        _, import_stock, expect_stock = SsStockPoint(sale_mean, sale_std, transportday_mean, 0, replenish_period, k)
        expect_stock = int(expect_stock)
        import_stock = int(import_stock)
        if i%replenish_period == 0:

            if i < transportday_mean:
                stock = max( stock - sale_serie[sale_index[i-1]],0)
                if (stock < import_stock) & ((expect_stock - stock) >= min_replesh):
                    replenish_seri[sale_index[i]] = expect_stock -stock

                else:
                    replenish_seri[sale_index[i]] = 0
                stock_seri[sale_index[i]] = stock
            else:
                stock = stock + replenish_seri[sale_index[i-transportday_mean]]
                stock = max( stock - sale_serie[sale_index[i-1]],0)
                if (stock < import_stock) & ((expect_stock - stock) >= min_replesh):
                    replenish_seri[sale_index[i]] = expect_stock -stock

                else:
                    replenish_seri[sale_index[i]] = 0
                stock_seri[sale_index[i]] = stock
        else:
            if i < transportday_mean:
                stock = max( stock - sale_serie[sale_index[i-1]],0)
                replenish_seri[sale_index[i]] = 0
                stock_seri[sale_index[i]] = stock
            else:
                stock = stock + replenish_seri[sale_index[i-transportday_mean]]
                stock = max( stock - sale_serie[sale_index[i-1]],0)
                replenish_seri[sale_index[i]] = 0
                stock_seri[sale_index[i]] = stock

    return stock_seri,replenish_seri

def stock_simulation_1(sale_serie,replenish_period,transportday_mean,k,ini_stock=None,min_replesh=0,bp=0):


    predict_sale = simple_predict.simple_predict(sale_serie)
    sale_std = np.std(sale_serie)
    # accuracy = simple_predict.get_accuracy(sale_serie,predict_sale)
    accuracy = r2_score(sale_serie,predict_sale)
    sale_index = sale_serie.index
    if ini_stock is None:
        ini_stock = sum(predict_sale[sale_index[0]:sale_index[transportday_mean]])
    stock_seri = pd.Series(index=sale_index)
    replenish_seri = pd.Series(index=sale_index)
    stock_seri[sale_index[0]] = ini_stock
    sale_mean = np.mean(predict_sale[sale_index[0]:sale_index[replenish_period]])
    _ ,import_stock,expect_stock = SsStockPoint(sale_mean, sale_std, transportday_mean, 0,
                                    replenish_period, k)
    stock = ini_stock
    expect_stock = int(expect_stock)
    import_stock = int(import_stock)
    import_stock = import_stock + sum(predict_sale[sale_index[0]:sale_index[bp]])
    # expect_stock = expect_stock + sum(predict_sale[sale_index[0]:sale_index[bp]])
    if (stock < import_stock) & ((expect_stock - stock) >= min_replesh):
        replenish_seri[sale_index[0]] = int(expect_stock - stock)
    else:
        replenish_seri[sale_index[0]] = 0

    for i in np.arange(1,len(sale_index),1):

        last_day = (len(sale_index)-replenish_period-transportday_mean)
        if i <= last_day:
            sale_mean = np.mean(predict_sale[sale_index[i]:sale_index[i + replenish_period+transportday_mean-1]])
        else:
            sale_mean = np.mean(predict_sale[sale_index[last_day]:])

        _, import_stock, expect_stock = SsStockPoint(sale_mean, sale_std, transportday_mean, 0, replenish_period, k)
        expect_stock = int(expect_stock)
        import_stock = int(import_stock)
        if i+bp < len(sale_index)-1:
               import_stock = import_stock + sum(predict_sale[sale_index[i]:sale_index[bp+i]])
               # expect_stock = expect_stock + sum(predict_sale[sale_index[0]:sale_index[bp]])
        if i%replenish_period == 0:
            if i < transportday_mean:
                stock = max( stock - sale_serie[sale_index[i-1]],0)
                if (stock < import_stock) & ((expect_stock - stock) >= min_replesh):
                    replenish_seri[sale_index[i]] = expect_stock -stock
                else:
                    replenish_seri[sale_index[i]] = 0
                stock_seri[sale_index[i]] = stock
            else:
                stock = stock + replenish_seri[sale_index[i-transportday_mean]]
                stock = max( stock - sale_serie[sale_index[i-1]],0)
                if (stock < import_stock) & ((expect_stock - stock) >= min_replesh):
                    replenish_seri[sale_index[i]] = expect_stock -stock
                else:
                    replenish_seri[sale_index[i]] = 0
                stock_seri[sale_index[i]] = stock
        else:
            if i < transportday_mean:
                stock = max( stock - sale_serie[sale_index[i-1]],0)
                replenish_seri[sale_index[i]] = 0
                stock_seri[sale_index[i]] = stock
            else:
                stock = stock + replenish_seri[sale_index[i-transportday_mean]]
                stock = max( stock - sale_serie[sale_index[i-1]],0)
                replenish_seri[sale_index[i]] = 0
                stock_seri[sale_index[i]] = stock

    return stock_seri,replenish_seri,accuracy

def stock_simulation_2(sale_serie,replenish_period,transportday_mean,k,ini_stock=None,min_replesh=0,bp=0,nrtk=1, min_except=0 ):

    predict_sale = simple_predict.simple_predict(sale_serie)
    sale_std = np.std(sale_serie)
    # accuracy = simple_predict.get_accuracy(sale_serie,predict_sale)
    accuracy = r2_score(sale_serie,predict_sale)
    sale_index = sale_serie.index
    if ini_stock is None:
        ini_stock = sum(predict_sale[sale_index[0]:sale_index[transportday_mean]])
    stock_seri = pd.Series(index=sale_index)
    replenish_seri = pd.Series(index=sale_index)
    stock_seri[sale_index[0]] = ini_stock
    sale_mean = np.mean(predict_sale[sale_index[0]:sale_index[replenish_period+transportday_mean+bp]])
    _ ,import_stock,expect_stock = pro_SsStockPoint(sale_mean, sale_std, transportday_mean, 0,
                                    replenish_period, k, nrtk)
    stock = ini_stock
    expect_stock = int(expect_stock)
    import_stock = int(import_stock)
    expect_stock = expect_stock + sum(predict_sale[sale_index[replenish_period+transportday_mean]:sale_index[replenish_period+transportday_mean+bp]])
    expect_stock = int(max(expect_stock,min_except))
    if (stock <= import_stock) & ((expect_stock - stock) >= min_replesh):
        replenish_seri[sale_index[0]] = int(expect_stock - stock)
    else:
        replenish_seri[sale_index[0]] = 0

    for i in np.arange(1,len(sale_index),1):

        last_day = (len(sale_index)-replenish_period-transportday_mean-bp)
        if i <= last_day:
            sale_mean = np.mean(predict_sale[sale_index[i]:sale_index[i + replenish_period+transportday_mean+bp-1]])
        else:
            sale_mean = np.mean(predict_sale[sale_index[last_day]:])
        _, import_stock, expect_stock = pro_SsStockPoint(sale_mean, sale_std, transportday_mean, 0, replenish_period, k, nrtk)
        expect_stock = int(expect_stock)
        import_stock = int(import_stock)
        if i+bp+replenish_period+transportday_mean < len(sale_index)-1:
               expect_stock = expect_stock + sum(predict_sale[sale_index[replenish_period+transportday_mean]:sale_index[replenish_period+transportday_mean+bp]])
        expect_stock = int(max(expect_stock, min_except))
        if i%replenish_period == 0:
            if i < transportday_mean:
                stock = max( stock - sale_serie[sale_index[i-1]],0)
                if (stock <= import_stock) & ((expect_stock - stock) >= min_replesh):
                    replenish_seri[sale_index[i]] = expect_stock -stock
                else:
                    replenish_seri[sale_index[i]] = 0
                stock_seri[sale_index[i]] = stock
            else:
                stock = stock + replenish_seri[sale_index[i-transportday_mean]]
                stock = max( stock - sale_serie[sale_index[i-1]],0)
                if (stock <= import_stock) & ((expect_stock - stock) >= min_replesh):
                    replenish_seri[sale_index[i]] = expect_stock -stock
                else:
                    replenish_seri[sale_index[i]] = 0
                stock_seri[sale_index[i]] = stock
        else:
            if i < transportday_mean:
                stock = max( stock - sale_serie[sale_index[i-1]],0)
                replenish_seri[sale_index[i]] = 0
                stock_seri[sale_index[i]] = stock
            else:
                stock = stock + replenish_seri[sale_index[i-transportday_mean]]
                stock = max( stock - sale_serie[sale_index[i-1]],0)
                replenish_seri[sale_index[i]] = 0
                stock_seri[sale_index[i]] = stock

    return stock_seri,replenish_seri,accuracy

if __name__ == '__main__':

    df = pd.read_csv('../data/cal_data/simulation_input.csv',dtype={'date':str})
    # df['date'] = df['date'].astype
    df = df.set_index('date')
    df = df.fillna(0)
    # stock_seri, replenish_seri ,predict_sale= stock_simulation_2(df['100186CH'],7,3,0.90,0,)
    stock_seri, replenish_seri ,predict_sale= stock_simulation_2(sale_serie =df['100186CH'] ,replenish_period= 7,transportday_mean=3,
                                                                 k =0.8,ini_stock=None,min_replesh=0,bp=0,nrtk=0.5)
    result = pd.DataFrame(df['100186CH'])
    result['stock'] = stock_seri
    result['predict'] = predict_sale
    result['replenish'] = replenish_seri

    df.columns
