# -*- coding: utf-8 -*-
# Created by sunze  on 2018/9/25

import pandas as pd
import dask.dataframe as dd
from dask.multiprocessing import get
import datetime

class stores_info(object):

    def __init__(self, file_path='../data/cal_data/store_toC.csv'):
        self.data = pd.read_csv(file_path)
        self.storelist = [self.data['store_id']]
        self.data = self.data.set_index('store_id')

    def isFDC(self,store):
        return self.data.loc[store,'FDC/RDC']== 'FDC'

    def get_support_storeid(self,store):

        if self.isFDC(store):
            return self.data.loc[store,'support_storeid']
        else:
            return False

    def get_support_DC(self,store):

        if self.isFDC(store):
            return self.data.loc[store,'support_DC']
        else:
            return False

    def is_inJD(self,store):
        return self.data.loc[store,'JDorNOT']== 'Y'

class stores_tocity_info(object):

    def __init__(self, file_path='../data/cal_data/store_to_city.csv'):
        self.data = pd.read_csv(file_path)
        self.data = self.data.set_index(['end_province','end_city'])

    def get_store(self,end_province,end_city):
        return self.data.loc[end_province,end_city]['store_id']

    def is_inJD(self,end_province,end_city):
        return self.data.loc[end_province,end_city]['JDorNOT']== 'Y'

class sku_inFDC_info(object):

    def __init__(self, file_path='../data/cal_data/split_sku6%.csv'):
        self.data = pd.read_csv(file_path)
        self.data = self.data.applymap(lambda x: str(x).strip())

    def get_skulist(self,store_city):
        return set(self.data[store_city].dropna())


if __name__ == '__main__':
    #
    # stores_tocity = stores_tocity_info()
    # citys = stores_tocity.data.reset_index()
    # # sale_data = pd.read_csv('../data/cal_data/toc/sale_2c_new.csv')
    # sale_data = pd.read_csv('../data/cal_data/toc/sale_2c_new.csv')
    # print('读取完毕！！')
    # sale_data['sku_id'] = sale_data['sku_id'].apply(lambda x: str(x).strip())
    # data = pd.merge(sale_data,citys,left_on=['end_province','end_city'], right_on=['end_province','end_city'])
    # # sale_data = sale_data[sale_data['JDorNOT']=='Y']
    # # sale_data.to_csv('../data/cal_data/sale_2c_new.csv')
    # # data = data.sample(frac=1)
    #
    # data = data[[ 'order_id', 'sku_id', 'date', 'qty', 'store_id_x',
    #    'start_province', 'start_city_x', 'end_province', 'end_city',
    #     'JDorNOT_x', 'store_id_y', 'store_city',
    #    'start_city_y', 'FDC/RDC', 'support_DC', 'support_storeid',
    #    'JDorNOT_y']]
    # sku_inFDC = sku_inFDC_info()
    # data1 = data.copy()
    # print('读取完毕2！！')
    # def split_row(row):
    #     if row['FDC/RDC'] == 'FDC':
    #         if row['sku_id'] not in sku_inFDC.get_skulist(row['store_cit                               y']):
    #             # print(row['sku_id'] + row['store_city'])
    #             row['order_id'] = str(row['order_id']) + '_1'
    #             row['store_id_y'] = row['support_storeid']
    #             row['store_city'] = row['support_DC']
    #     return row
    #
    # time1 = datetime.datetime.now()
    # ddata = dd.from_pandas(data1, npartitions=10)
    # data2 = ddata.map_partit ions(lambda df: df.apply((lambda row: split_row(row)), axis=1)).compute(get=get)
    #
    # # data2 = data1.apply(split_row,axis=1)
    #
    # time2 = datetime.datetime.now()
    # print((time2-time1).seconds)
    # data2.to_csv('../data/cal_data/sale_2c_splited6%.csv')


    # stores_tocity = stores_tocity_info()
    # citys = stores_tocity.data.reset_index()
    # sale_data = pd.read_csv('../data/cal_data/toc/sale_2c_new.csv')
    # print('读取完毕！！')
    # sale_data['sku_id'] = sale_data['sku_id'].apply(lambda x: str(x).strip())
    # data = pd.merge(sale_data,citys,left_on=['end_province','end_city'], right_on=['end_province','end_city'])
    import math
    import numpy as np

    goods_layout = '24'
    print(goods_layout)
    df_toc_statis = pd.read_csv('../data/toc_statistic.csv',low_memory=False)
    cover_citys = pd.read_csv('../data/goods_layout_{}/cover_citys.csv'.format(goods_layout))
    df_toc_statis_match = df_toc_statis.merge(cover_citys[['start_province', 'start_city', 'end_province',
                                                             'end_city']], how='outer', left_on='dis_city',
                                              right_on='end_city')
    df_toc_statis_match[['dis_province', 'dis_city', 'start_province','start_city', 'date', 'sku_id', 'qty']].to_csv(
                                               '../data/goods_layout_{}/toc_statistic_match.csv'.format(goods_layout))
    df_toc_statis_match_na = df_toc_statis_match[df_toc_statis_match['end_city'].isna()]
    df_toc_order =  pd.read_csv('../data/toc_orders_new.csv',low_memory=False)
    df_toc_order_left = df_toc_order[['id']].drop_duplicates()[:5740000]
    df_toc_order = df_toc_order.merge(df_toc_order_left,how='inner')
    cover_citys = cover_citys[['start_province', 'start_city', 'end_province', 'end_city', 'vlt']]
    toc_order_match = df_toc_order[[ 'id', 'sku_id', 'date', 'qty', 'dis_province', 'dis_city', 'price']].merge(cover_citys,left_on='dis_city',right_on='end_city',how='left')
    toc_order_match.to_csv('../data/goods_layout_{}/toc_order_match.csv'.format(goods_layout))
    # fdc = pd.read_csv('../data/goods_layout_{}/RDC-FDC.csv'.format(goods_layout))
    # cover_shops = cover_citys[['start_province', 'start_city', 'end_province', 'end_city',
    #                                'vlt']].merge(fdc, how='outer', left_on='start_city', right_on='FDC',indicator=True)
    # cover_shops['VLT']=0
    # for index in cover_shops.index:
    #     if cover_shops.loc[index,'_merge']=='left_only':
    #         cover_shops.loc[index,'RDC'] = cover_shops.loc[index,'start_city']
    #         cover_shops.loc[index,'vlt_y'] = 0
    #     cover_shops.loc[index,'VLT'] = cover_shops.loc[index,'vlt_x'] + int(math.ceil(cover_shops.loc[index,'vlt_y']/5))
    # shops = pd.read_csv('../data/shops_new.csv')
    # cover_shops = cover_shops.drop_duplicates()
    # shops.merge(cover_shops[['end_province', 'end_city','RDC', 'FDC','VLT']],left_on='city',right_on='end_city').to_csv('../data/goods_layout_{}/shops_match.csv'.format(goods_layout))
    # df_shop_toc_statis = pd.read_csv('../data/shop_toc_statistics.csv')
    # df_shop_toc_statis['city'] = df_shop_toc_statis['city'].apply(lambda x:x.strip())
    # df_shop_toc_statis['province'] = df_shop_toc_statis['province'].apply(lambda x:x.strip())
    # df_shop_toc_statis_match = df_shop_toc_statis[['shop_id', 'shop_name', 'province', 'city', 'month','date', 'sku_id',
    #                                                 'qty']].merge(cover_shops[['end_province', 'end_city','RDC']],
    #                                                 how='inner',left_on='city',right_on='end_city',indicator=True)
    # df_shop_toc_statis_match[['shop_id', 'shop_name', 'province', 'city', 'month','date', 'sku_id',
    #                                                 'qty','RDC']].to_csv('../data/goods_layout_{}/shop_toc_statistic_match.csv'.format(goods_layout))
