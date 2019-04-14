# -*- coding: utf-8 -*-
# Created by todoit on 2018/9/27

import pandas as pd
import numpy as np
import math
from os import listdir
from os.path import isfile, join
import sys


import dask.dataframe as dd
import dask.multiprocessing

desired_width = 320
pd.set_option('display.width', desired_width)

class AnliSimulation(object):

    def __init__(self, percent_dir, split_pallet_num):
        """
        初始化
        """

        self.percent_dir = percent_dir
        self.split_pallet_num = split_pallet_num * 1.0
        print('当前计算比例:{}'.format(self.percent_dir))
        print('当前托盘拆分单位为1/{}'.format(self.split_pallet_num))

        # 当前日期
        self.today = ""

        # 库存
        # store_id, is_rdc, is_fdc, sku_id, sku_count
        self.stock_df = None

        # sku信息
        # sku_id, length, width, height, volume, weight
        # self.sku_df = pd.read_csv('../data/cal_data/sku/sku_Info.csv',
        self.sku_df = pd.read_csv('../data/cal_data/sku/sku_Info_eclp.csv',
                                  encoding='utf-8',
                                  engine='python',
                                  sep=',',
                                  usecols=['sku', 'num_preplate', 'plate_weight_kg', 'temperature_control', 'dangerous_goods', 'weight'],
                                  dtype={'qty': int}
                                  )
        print('sku_df read over!')
        print('sku 去重前{}'.format(self.sku_df.shape))
        self.sku_df['sku'] = self.sku_df['sku'].apply(lambda x: x.strip())
        self.sku_df = self.sku_df.drop_duplicates(subset=['sku'])
        print('sku 去重后{}'.format(self.sku_df.shape))

        # 补货、存储 费用信息
        self.cost_tmplate_df = pd.read_csv('../data/cal_data/仓储以及补货报价.csv',
                                  # encoding='utf-8',
                                  engine='python',
                                  sep=',',
                                  usecols=['store_id','store_province', 'store_city', '17年装卸费报价', '17年普通品补货运费报价', '17年恒温品补货运费报价', '17年危险品补货运费报价', '17年普通品仓租费报价', '17年恒温品仓租费报价', '17年危险品仓租费报价'],
                                  dtype={'num_preplate':np.float_}
                                  )

        print('cost_tmplate_df read over!')

        # 销量数据，用来计算补货
        # date, store_id, is_rdc, is_fdc, sku_count
        self.sale_df = None

        # 仓储费df_shoptoc = df_shoptoc.merge(shops,how='inner',le)
        # store_id, cost
        self.storage_cost_df = None

        # 运输费模板
        # start_store_id, target_store_id, cost
        self.transport_cost_df = None

        # 装卸费模板
        # store_id, cost
        self.handling_cost_df = None

        # 配送费模板
        self.delivery_cost_df = None

        # 总费用表格
        # date, storage_cost, transport_cost, handling_cost, delivery_cost
        self.all_cost_record = None



    def delivery(self):
        """
        配送
        """
        pass

    def replenishment(self):
        """
        补货
        :return:
        """
        pass



    def cal_delivery_cost(self):
        """
        根据sku数量和仓库计算配送费
        :param sku_count_df: store_id, sku_id, sku_count
        :return: double
        """
        self.sale_df = pd.read_csv('../data/cal_data/toc/sale_2c_splited6%.csv',
                                   encoding='utf-8',
                                   engine='python',
                                   na_filter=False,
                                   low_memory=True,
                                   sep=',',
                                   usecols=['order_id', 'sku_id', 'qty', 'start_province', 'end_province', 'store_id_y'],
                                   dtype={'order_id':str, 'sku_id':str, 'qty':int, 'start_province':str, 'end_province':str, 'store_id_y':str})
        print('sale_df read over!')
        # sale_df1['order_id_origin'] = sale_df1.map_partitions(lambda x: lambda x: x.split('_')[0] if '_' in x else x).compute()
        # self.sale_df = sale_df1.compute(get=get)
        # print('over1')
        # 原来order_id
        self.sale_df['order_id_origin'] = self.sale_df['order_id'].apply(lambda x: x.split('_')[0] if '_' in x else x)
        # self.sale_df['order_id_origin'] = self.sale_df['order_id'].map_partitions(lambda df: df['order_id'].apply((lambda x: x.split('_')[0] if '_' in x else x)).compute(get=get))
        print('order_id over!')

        group1 = self.sale_df[['end_province','qty']].groupby('end_province').agg({'qty':np.sum})
        sum1 = group1['qty'].sum()
        print('sum1')
        print(sum1)
        print(group1)



        # 配送费 首重 8元/kg，续重0.7/0.5kg
        tmp_delivery_cost_first_weight = {'price': 8.0, 'unit': 1.0}
        tmp_delivery_cost_continue_weight = {'price': 0.7, 'unit': 0.5}

        # 新疆和西藏
        tmp_delivery_cost_first_weight_xj_xz = {'price': 12.0, 'unit': 1.0}
        tmp_delivery_cost_continue_weight_xj_xz = {'price': 1.2, 'unit': 0.5}

        # 分拣费 元/单
        tmp_picking_cost_unit = 14.31

        # 包装费 元/票
        tmp_package_cost_unit = 5.87

        # 合并df
        sale_sku_df = pd.merge(left=self.sale_df, right=self.sku_df, left_on='sku_id', right_on='sku', how='left')

        # 计算总的父单数
        parent_order_count = len(self.sale_df['order_id_origin'].unique().tolist())
        print("拆单前的父单数:{}".format(parent_order_count))

        # 计算总分拣费
        picking_cost_sum = tmp_picking_cost_unit * parent_order_count
        print("总分拣费:{}".format(picking_cost_sum))

        # 计算拆单之后的总单数
        split_order_count = len(self.sale_df['order_id'].unique().tolist())
        print("拆单后的父单数:{}".format(split_order_count))

        # 计算包装费
        package_cost_sum = tmp_package_cost_unit * split_order_count
        print("总包装费:{}".format(package_cost_sum))

        # 计算每个sku的重量
        sale_sku_df['weight_sum'] = sale_sku_df['weight'] * sale_sku_df['qty']

        # 计算每拆单后每个订单的重量
        split_order_group = sale_sku_df.groupby(['order_id','end_province']).agg({'weight_sum': np.sum}).reset_index()

        # 每个地方的运费单价
        split_order_group['first_weight_price'] = tmp_delivery_cost_first_weight['price']
        split_order_group['first_weight_unit'] = tmp_delivery_cost_first_weight['unit']
        split_order_group['continue_weight_price'] = tmp_delivery_cost_continue_weight['price']
        split_order_group['continue_weight_unit'] = tmp_delivery_cost_continue_weight['unit']

        # 新疆和西藏的报价
        split_order_group.loc[split_order_group['end_province'] == '新疆自治区', 'first_weight_price'] = tmp_delivery_cost_first_weight_xj_xz['price']
        split_order_group.loc[split_order_group['end_province'] == '新疆自治区', 'first_weight_unit'] = tmp_delivery_cost_first_weight_xj_xz['unit']
        split_order_group.loc[split_order_group['end_province'] == '新疆自治区', 'continue_weight_price'] = tmp_delivery_cost_continue_weight_xj_xz['price']
        split_order_group.loc[split_order_group['end_province'] == '新疆自治区', 'continue_weight_unit'] = tmp_delivery_cost_continue_weight_xj_xz['unit']
        print('西藏和新疆订单个数为:{}'.format(split_order_group.loc[split_order_group['end_province'] == '新疆自治区'].shape))

        dtype0 = {'first_weight_price': np.float_,
                  'first_weight_unit': np.float_,
                  'continue_weight_price': np.float_,
                  'continue_weight_unit': np.float_,
                  }
        split_order_group = split_order_group.astype(dtype0)
        # 计算首重价格

        # 计算每个订单的配送费
        split_order_group['delivery_cost_all'] = split_order_group.apply(lambda row: self.delivery_cost_all(row), axis=1)
        # split_order_group['delivery_cost_sum'] = split_order_group['weight_sum'].apply(lambda x: self.delivery_cost_all(x, tmp_delivery_cost_first_weight, tmp_delivery_cost_continue_weight))

        # 拆单后的订单配送费
        delivery_cost_sum = split_order_group['delivery_cost_all'].sum()
        print("拆单后的订单配送费为:{}".format(delivery_cost_sum))

        # 最终配送费为
        final_delivery_cost = picking_cost_sum + package_cost_sum + delivery_cost_sum
        print('最终配送费为：{}'.format(final_delivery_cost))



        print('OVER')

    def delivery_cost_all(self, row):
        if row['weight_sum'] <= row['first_weight_unit']:
            return row['first_weight_price']
        else:
            return row['first_weight_price'] + (math.ceil((row['weight_sum'] - row['first_weight_unit']) /
                                                          row['continue_weight_unit']) * row['continue_weight_price'])

    def cal_delivery_toB_cost(self):

        tob_data = pd.read_csv('../data/cal_data/tob/sale_2b_order.csv',usecols = ['order_id', 'sku', 'date', 'qty',
                             'start_city', 'end_province', 'end_city', '17年运输报价', '18年运输报价'])
        tob_data['sku'] = tob_data['sku'].apply(lambda x: str(x).strip())
        merged_tob_df = pd.merge(left=tob_data, right=self.sku_df[['sku','num_preplate']],left_on='sku', right_on='sku',
                                       how='left')
        merged_tob_df['sku_plate_num'] = merged_tob_df.apply(lambda row: round(row['qty'] / row['num_preplate'], 1),
                                                             axis=1)
        grouped_tob_df = merged_tob_df[['order_id','start_city','end_province','end_city','17年运输报价','sku_plate_num']]\
                        .groupby(['order_id','start_city','end_province','end_city','17年运输报价'],
                        as_index=False).agg(sum)
        grouped_tob_df['num_preplate'] = grouped_tob_df.apply(lambda row: round((math.ceil(
                                               row['sku_plate_num']*self.split_pallet_num) * 1.0) / self.split_pallet_num,
                                               1), axis=1)
        print('toB费用计算----')
        # 计算运输价格
        grouped_tob_df['delivery_tob_cost'] = grouped_tob_df['17年运输报价'] * grouped_tob_df['num_preplate']
        # 计算装卸价格
        grouped_tob_df['handling_sum'] =grouped_tob_df['num_preplate'] * 28.0

        grouped_tob_df['delivery_tob_cost_all'] = grouped_tob_df['delivery_tob_cost']+grouped_tob_df['handling_sum']
        # 分组求每个仓库的补货成本
        merged_grouped_tob_df = grouped_tob_df[['start_city', 'delivery_tob_cost', 'handling_sum', 'delivery_tob_cost_all']]\
                       .groupby('start_city').agg({'delivery_tob_cost':np.sum, 'handling_sum':np.sum, 'delivery_tob_cost_all': np.sum})
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(merged_grouped_tob_df)
        print('装卸费最终成本为:{}'.format(merged_grouped_tob_df['handling_sum'].sum()))
        print('运费最终成本为:{}'.format(merged_grouped_tob_df['delivery_tob_cost'].sum()))
        print('==================')
        print('运费+装卸费最终成本为:{}'.format(merged_grouped_tob_df['delivery_tob_cost_all'].sum()))

        print('=================运费 {} OVER ======================'.format(self.percent_dir))

    def cal_transport_cost(self):
        """
        根据sku数量，出发仓库和目的仓库计算运费
        :param sku_count_df: start_store_id, target_store_id, sku_id, sku_count
        :return:
        """

        # 得到所有补货文件
        print()
        print('============开始计算补货费（运输费和装卸费） {}==========='.format(self.percent_dir))
        replenish_file_path = self.percent_dir
        print('当前计算文件夹：{}'.format(replenish_file_path))
        replenish_files = [f for f in listdir(replenish_file_path) if 'replenish' in f and isfile(join(replenish_file_path, f))]
        print('文件个数====== {}'.format(len(replenish_files)))
        # 读取补货数据
        replenish_df_list = []
        for i, replenish_file in enumerate(replenish_files):
            tmp_replenish_df = pd.read_csv(join(replenish_file_path, replenish_file),
                                       encoding='utf-8',
                                       engine='python',
                                       sep=',',
                                       dtype={0: np.str_}
                                       )
            tmp_replenish_df.rename(columns={tmp_replenish_df.columns[0]: 'date'}, inplace=True)
            # 转换成三元组形式
            tmp_replenish_df = pd.melt(tmp_replenish_df,
                                   id_vars='date',
                                   value_vars=list(tmp_replenish_df.columns[1:]),  # list of days of the week
                                   var_name='sku_id',
                                   value_name='sku_qty')
            # 增加仓库名称
            tmp_replenish_df['store_city'] = replenish_file.split('_')[2].split('.')[0]
            shape0 = tmp_replenish_df.shape[0]
            # 删除数量是0的数据
            tmp_replenish_df = tmp_replenish_df.loc[tmp_replenish_df['sku_qty'] > 0]
            print('{}:  {} \t\t{} ------>  {} '.format(i + 1, replenish_file, shape0, tmp_replenish_df.shape[0]))
            # print('删除数量为0的记录后，shape为{}'.format(tmp_replenish_df.shape))
            replenish_df_list.append(tmp_replenish_df)

        # 合并为一个文件
        replenish_all_df = pd.concat(replenish_df_list)

        # merge sku info
        merged_replenish_df = pd.merge(left=replenish_all_df, right=self.sku_df, left_on='sku_id', right_on='sku', how='left')
        # merge warehouse price info
        merged_replenish_df = pd.merge(left=merged_replenish_df, right=self.cost_tmplate_df, left_on='store_city', right_on='store_city', how='left')
        # 判断是否有sku或者仓库信息找不到
        null_sku_df = merged_replenish_df.loc[merged_replenish_df['sku'].isnull() | (merged_replenish_df['sku'] == 'NaN') | merged_replenish_df['17年装卸费报价'].isnull()]
        if null_sku_df.shape[0] > 0:
            # print('存在sku或者仓库找不到的情况')
            print('程序退出')
            sys.exit(-1)

        # 计算每个sku的托盘个数，精确到0.5
        merged_replenish_df['sku_plate_num'] = merged_replenish_df.apply(lambda row: round((math.ceil(row['sku_qty'] / (row['num_preplate'] / self.split_pallet_num)) * 1.0) / self.split_pallet_num, 1), axis=1)
        print('使用托盘个数计算完成')
        # 根据sku类型计算该sku的运费报价
        merged_replenish_df['use_transport_price'] = merged_replenish_df.apply(lambda row: self.select_sku_replenish_price(row), axis=1)
        merged_replenish_df['use_handling_price'] = merged_replenish_df['17年装卸费报价'] * 1.0
        print('使用运费价格计算完成')
        # 计算运输价格
        merged_replenish_df['replenish_transport_sum'] = merged_replenish_df.use_transport_price * merged_replenish_df.sku_plate_num
        # 计算装卸价格
        merged_replenish_df['replenish_handling_sum'] = merged_replenish_df.use_handling_price * merged_replenish_df.sku_plate_num
        # 总价格
        merged_replenish_df['replenish_cost_sum'] = merged_replenish_df.replenish_handling_sum + merged_replenish_df.replenish_transport_sum

        print('总价格计算完成')
        # 分组求每个仓库的补货成本
        merged_replenish_grouped = merged_replenish_df[['store_city', 'replenish_transport_sum', 'replenish_handling_sum', 'replenish_cost_sum']].groupby('store_city').agg({'replenish_transport_sum':np.sum, 'replenish_handling_sum':np.sum, 'replenish_cost_sum': np.sum})
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(merged_replenish_grouped)
        print('装卸费最终成本为:{}'.format(merged_replenish_grouped['replenish_handling_sum'].sum()))
        print('运费最终成本为:{}'.format(merged_replenish_grouped['replenish_transport_sum'].sum()))
        print('==================')
        print('运费+装卸费最终成本为:{}'.format(merged_replenish_grouped['replenish_cost_sum'].sum()))

        print('=================运费 {} OVER ======================'.format(self.percent_dir))

    def cal_transport_cost_1(self):
        """
        根据sku数量，出发仓库和目的仓库计算运费
        :param sku_count_df: start_store_id, target_store_id, sku_id, sku_count
        :return:
        """

        # 得到所有补货文件
        print()
        print('============开始计算补货费（运输费和装卸费） {}==========='.format(self.percent_dir))
        replenish_file_path = self.percent_dir
        print('当前计算文件夹：{}'.format(replenish_file_path))
        replenish_files = [f for f in listdir(replenish_file_path) if 'replenish' in f and isfile(join(replenish_file_path, f))]
        print('文件个数====== {}'.format(len(replenish_files)))
        # 读取补货数据
        replenish_df_list = []
        for i, replenish_file in enumerate(replenish_files):
            tmp_replenish_df = pd.read_csv(join(replenish_file_path, replenish_file),
                                       encoding='utf-8',
                                       engine='python',
                                       sep=',',
                                       dtype={0: np.str_}
                                       )
            tmp_replenish_df.rename(columns={tmp_replenish_df.columns[0]: 'date'}, inplace=True)
            # 转换成三元组形式
            tmp_replenish_df = pd.melt(tmp_replenish_df,
                                   id_vars='date',
                                   value_vars=list(tmp_replenish_df.columns[1:]),  # list of days of the week
                                   var_name='sku_id',
                                   value_name='sku_qty')
            # 增加仓库名称
            tmp_replenish_df['store_city'] = replenish_file.split('_')[2].split('.')[0]
            shape0 = tmp_replenish_df.shape[0]
            # 删除数量是0的数据
            tmp_replenish_df = tmp_replenish_df.loc[tmp_replenish_df['sku_qty'] > 0]
            print('{}:  {} \t\t{} ------>  {} '.format(i + 1, replenish_file, shape0, tmp_replenish_df.shape[0]))
            # print('删除数量为0的记录后，shape为{}'.format(tmp_replenish_df.shape))
            replenish_df_list.append(tmp_replenish_df)

        # 合并为一个文件
        replenish_all_df = pd.concat(replenish_df_list)

        # merge sku info
        merged_replenish_df = pd.merge(left=replenish_all_df, right=self.sku_df, left_on='sku_id', right_on='sku', how='left')
        # merge warehouse price info
        merged_replenish_df = pd.merge(left=merged_replenish_df, right=self.cost_tmplate_df, left_on='store_city', right_on='store_city', how='left')
        # 判断是否有sku或者仓库信息找不到
        null_sku_df = merged_replenish_df.loc[merged_replenish_df['sku'].isnull() | (merged_replenish_df['sku'] == 'NaN') | merged_replenish_df['17年装卸费报价'].isnull()]
        if null_sku_df.shape[0] > 0:
            # print('存在sku或者仓库找不到的情况')
            print('程序退出')
            sys.exit(-1)

        # 计算每个sku的托盘个数，精确到0.5
        merged_replenish_df['sku_plate_num'] = merged_replenish_df.apply(lambda row: round(row['sku_qty'] / row['num_preplate'], 1), axis=1)
        print('使用托盘个数计算完成')
        # 根据sku类型计算该sku的运费报价
        merged_replenish_df = merged_replenish_df[
            ['date', 'store_city', 'temperature_control', 'dangerous_goods', '17年装卸费报价', '17年普通品补货运费报价', '17年恒温品补货运费报价',
             '17年危险品补货运费报价', '17年普通品仓租费报价', '17年恒温品仓租费报价', '17年危险品仓租费报价', 'sku_plate_num']].groupby(
            ['date', 'store_city', 'temperature_control', 'dangerous_goods', '17年装卸费报价', '17年普通品补货运费报价', '17年恒温品补货运费报价',
             '17年危险品补货运费报价', '17年普通品仓租费报价', '17年恒温品仓租费报价', '17年危险品仓租费报价'], as_index=False).agg(sum)
        merged_replenish_df['sku_plate_num'] = merged_replenish_df.apply(lambda row: round((math.ceil(
                                               row['sku_plate_num']*self.split_pallet_num) * 1.0) / self.split_pallet_num,
                                               1), axis=1)

        merged_replenish_df['use_transport_price'] = merged_replenish_df.apply(lambda row: self.select_sku_replenish_price(row), axis=1)
        merged_replenish_df['use_handling_price'] = merged_replenish_df['17年装卸费报价'] * 1.0
        print('使用运费价格计算完成')
        # 计算运输价格
        merged_replenish_df['replenish_transport_sum'] = merged_replenish_df.use_transport_price * merged_replenish_df.sku_plate_num
        # 计算装卸价格
        merged_replenish_df['replenish_handling_sum'] = merged_replenish_df.use_handling_price * merged_replenish_df.sku_plate_num
        # 总价格
        merged_replenish_df['replenish_cost_sum'] = merged_replenish_df.replenish_handling_sum + merged_replenish_df.replenish_transport_sum

        print('总价格计算完成')
        # 分组求每个仓库的补货成本
        merged_replenish_grouped = merged_replenish_df[['store_city', 'replenish_transport_sum', 'replenish_handling_sum']].groupby('store_city').agg({'replenish_transport_sum':np.sum, 'replenish_handling_sum':np.sum})
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(merged_replenish_grouped)
        print('装卸费最终成本为:{}'.format(merged_replenish_grouped['replenish_handling_sum'].sum()))
        print('运费最终成本为:{}'.format(merged_replenish_grouped['replenish_transport_sum'].sum()))
        print('==================')
        # print('运费+装卸费最终成本为:{}'.format(merged_replenish_grouped['replenish_cost_sum'].sum()))
        print('=================运费 {} OVER ======================'.format(self.percent_dir))

        return merged_replenish_grouped


    def select_sku_replenish_price(self, row):
        if row['dangerous_goods'] == 'Y':
            return row['17年危险品补货运费报价'] * 1.0
        if row['temperature_control'] == 'Y':
            return row['17年恒温品补货运费报价'] * 1.0
        else:
            return row['17年普通品补货运费报价'] * 1.0



    def cal_storage_cost(self):
        """
        根据sku数量和仓库计算仓储费
        :param sku_count_df: store_id, sku_id, sku_count
        :return:
        """

        # 得到所有库存文件
        print()
        print('============开始计算库存成本 {} ==============='.format(self.percent_dir))
        stock_file_path = self.percent_dir
        print('当前计算文件夹：{}'.format(stock_file_path))
        stock_files = [f for f in listdir(stock_file_path) if 'stock' in f and isfile(join(stock_file_path, f))]
        print('文件个数:{}'.format(len(stock_files)))
        # 读取库存数据
        stock_df_list = []
        for i, stock_file in enumerate(stock_files):
            tmp_stock_df = pd.read_csv(join(stock_file_path, stock_file),
                                      encoding='utf-8',
                                      engine='python',
                                      sep=',',
                                      dtype={0: np.str_}
                                      )
            tmp_stock_df.rename(columns={tmp_stock_df.columns[0]: 'date'}, inplace=True)
            # 转换成三元组形式
            tmp_stock_df = pd.melt(tmp_stock_df,
                             id_vars='date',
                             value_vars=list(tmp_stock_df.columns[1:]),  # list of days of the week
                             var_name='sku_id',
                             value_name='sku_qty')
            # 增加仓库名称
            tmp_stock_df['store_city'] = stock_file.split('_')[2].split('.')[0]
            shape0 = tmp_stock_df.shape[0]
            # print('{}读取完成，shape{}'.format(stock_file, tmp_stock_df.shape))
            # 删除数量是0的数据
            tmp_stock_df = tmp_stock_df.loc[tmp_stock_df['sku_qty'] > 0]

            print('{}:  {} \t\t{} ------>  {} '.format(i+1, stock_file, shape0, tmp_stock_df.shape[0]))
            stock_df_list.append(tmp_stock_df)

        # 合并为一个文件
        stock_all_df = pd.concat(stock_df_list)

        print('合并完成，总数据量{}'.format(stock_all_df.shape))

        # merge sku info
        merged_stock_df = pd.merge(left=stock_all_df, right=self.sku_df, left_on='sku_id', right_on='sku', how='left')

        # merge warehouse price info
        merged_stock_df = pd.merge(left=merged_stock_df, right=self.cost_tmplate_df, left_on='store_city', right_on='store_city', how='left')

        print('merge 完成')
        # 判断是否有sku或者仓库信息找不到
        null_sku_df = merged_stock_df.loc[merged_stock_df['sku'].isnull() | merged_stock_df['17年装卸费报价'].isnull()]
        if null_sku_df.shape[0] > 0:
            print('存在sku或者仓库找不到的情况')
            print('程序退出')
            sys.exit(-1)
        # 计算每个sku的托盘个数，精确到0.5
        merged_stock_df['sku_plate_num'] = merged_stock_df.apply(lambda row: round((math.ceil(row['sku_qty'] / (row['num_preplate'] / self.split_pallet_num)) * 1.0) / self.split_pallet_num, 1), axis=1)
        print('使用托盘个数计算完成')
        # 根据sku类型计算该sku的报价
        merged_stock_df['use_stock_price'] = merged_stock_df.apply(lambda row: self.select_sku_stock_price(row), axis=1)
        print('使用价格计算完成')
        # 计算总价格
        merged_stock_df['stock_cost_sum'] = merged_stock_df.use_stock_price * merged_stock_df.sku_plate_num
        print('总价格计算完成')
        # 分组求每个仓库的仓储成本
        merged_stock_grouped = merged_stock_df[['store_city', 'stock_cost_sum']].groupby('store_city').agg({'stock_cost_sum': np.sum})
        # print(merged_stock_grouped)

        print('==================')
        print('库存费最终成本为:{}'.format(merged_stock_grouped['stock_cost_sum'].sum()))

        print('=================库存费 {} OVER ======================'.format(self.percent_dir))

        return  merged_stock_grouped



    def select_sku_stock_price(self, row):
        if row['dangerous_goods'] == 'Y':
            return row['17年危险品仓租费报价']
        if row['temperature_control'] == 'Y':
            return row['17年恒温品仓租费报价']
        else:
            return row['17年普通品仓租费报价']


    def cal_handling_cost(self, sku_count_df):
        """
        根据sku数量和仓库计算搬运费
        :param sku_count_df: store_id, sku_id, sku_count
        :return:
        """
        pass



if __name__ == '__main__':
    percent_dir1 = r'../data/cal_data/result/eclp/result3%'
    split_pallet_num1 = 2.0
    anli = AnliSimulation(percent_dir=percent_dir1, split_pallet_num=split_pallet_num1)
    # anli.cal_delivery_cost()
    anli.cal_storage_cost()
    anli.cal_transport_cost_1()
    # anli.cal_delivery_toB_cost()

