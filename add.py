# -*- coding: utf-8 -*-
# Created by sunze  on 2018/12/24
import pandas as pd
import inventoryCal
import AnliSimulation
import math
from datetime import datetime

if __name__ == '__main__':

    goods_layout = 2
    RDC_period = 15
    FDC_period = 5
    service_level = 0.90
    bp= 5
    nrtk = 1.0
    shop_period = 3
    shop_service_level = 0.95
    shop_bp=20
    shop_nrtk = 3.0
    RDC = '广州'

    parameter = {'RDC补货周期': RDC_period, 'FDC调拨周期': FDC_period, '服务水平': service_level, 'BP': bp, 'nrtk': nrtk,
                 'shop调拨周期': shop_period, 'shop服务水平': shop_service_level, 'shop_BP': shop_bp, 'shop_nrtk': shop_nrtk}
    RDC_info = pd.read_csv('../data/goods_layout_{}/RDC.csv'.format(goods_layout))
    FDC_info = pd.read_csv('../data/goods_layout_{}/FDC.csv'.format(goods_layout))
    RDC_list = RDC_info['RDC'].drop_duplicates().tolist()
    shop_info = pd.read_csv('../data/goods_layout_{}/shops_match.csv'.format(goods_layout))
    sku_info = pd.read_csv('../data/sku_info_new.csv')
    XS_sku_list = sku_info[sku_info['类型']=='XS饮料']['sku_id'].drop_duplicates().tolist()
    cdc_sku_list = sku_info[(sku_info['类型']=='XS饮料')|(sku_info['类型']=='外购品')]['sku_id'].drop_duplicates().tolist()
    f = open('../data/goods_layout_{}/sale/RDC_广州.csv'.format(goods_layout))
    rdc_sale =  pd.read_csv(f,index_col=0)
    rdc_sale = rdc_sale.fillna(0)
    stock_df = pd.DataFrame(index=rdc_sale.index)
    replenish_df = pd.DataFrame(index=rdc_sale.index)
    # print('RDC：{} 正在模拟库存！'.format(city))

    for sku in list(rdc_sale.columns):

        stock_df[sku], replenish_df[sku],_ = inventoryCal.stock_simulation_2(rdc_sale[sku],RDC_period,
                                                                                                       1,
                                                                                                       k=service_level,
                                                                                                       bp=bp, nrtk=nrtk)
    if RDC != '广州':
        cdc_sale = rdc_sale.add(replenish_df, fill_value=0)
    # rdc_sale.to_csv('../data/goods_layout_{}/sale/RDC_{}.csv'.format(goods_layout, RDC))
    stock_df.to_csv('../data/goods_layout_{}/stock/RDC_{}.csv'.format(goods_layout, RDC))
    replenish_df.to_csv('../data/goods_layout_{}/repleshment/RDC_{}.csv'.format(goods_layout, RDC))
    print(stock_df.values.sum()/rdc_sale.values.sum())