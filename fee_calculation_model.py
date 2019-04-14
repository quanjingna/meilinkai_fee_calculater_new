# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
import math
from os import listdir
from os.path import isfile, join
import sys

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

class fee_calculation(object):

    def __init__(self, percent_dir=None):
        """
        初始化
        """
        self.percent_dir = percent_dir
        print('当前计算文件:{}'.format(self.percent_dir))

        # 当前日期
        self.today = ""
        self.stock_df = None
        self.sku_info_df = pd.read_csv('../data/fee/sku_info.csv',
                                  encoding='utf-8',
                                  )[['sku','normal','temperature_control','dangerous_goods','num_preplate','weight','plate_weight_kg']]
        self.skuclass_info_df = pd.read_excel('../data/fee/安利商品信息表1.xlsx')
        self.sku_info_df=pd.merge(self.sku_info_df,self.skuclass_info_df[['商家商品编码','类型']],left_on='sku',right_on='商家商品编码',how='left')
        self.sku_info_df['类型']=self.sku_info_df['类型'].fillna('外购品')
        # def function(a, b,c,d):
        #     if  a == 'Y':
        #         return '危险品'
        #     else:
        #         if b=='Y':
        #             return '恒温品'
        #         else:
        #             if c==d:
        #                 return 'XS饮料'
        #             else:
        #                 return '普通品'
        #
        # self.sku_info_df['类型_repl']=self.sku_info_df.apply(lambda x: function(x.loc['dangerous_goods'], x.loc['temperature_control'],x.loc['sku_id'],x.loc['sku']),axis=1)
        # #print(self.sku_info_df)
        #self.sku_info_df=self.sku_info_df[['sku', 'normal', 'temperature_control', 'dangerous_goods', 'num_preplate', 'weight', 'plate_weight_kg','类型']]
        self.sku_info_df=self.sku_info_df.rename(columns={'sku':'sku_id'})
        self.sku_info_df = self.sku_info_df[['sku_id','num_preplate','weight','plate_weight_kg','类型']]
        #print(self.sku_info_df)
        print('sku_info_df read over!')
        print('sku 去重前{}'.format(self.sku_info_df.shape))
        self.sku_info_df['sku_id'] = self.sku_info_df['sku_id'].apply(lambda x: x.strip())
        self.sku_info_df = self.sku_info_df.drop_duplicates(subset=['sku_id'])
        print('sku 去重后{}'.format(self.sku_info_df.shape))
        self.storage_fee_df  = pd.read_csv('../data/fee/storage_fee_new.csv', encoding='utf-8')
        print('仓费用storage_fee read over!')
        self.shipping_fee_df = pd.read_csv('../data/fee/shipping_fee_new.csv' )
        print('运费shipping_fee read over!')
        self.toc_fee_new=pd.read_csv('../data/fee/toc_fee_new.csv')
        print('配送费toc_fee_new read over!')
        self.danger_fee_new = pd.read_csv('../data/fee/danger_fee.csv')
        self.danger_fee_new['类型']='危险品'
        print('危险品RDC报价 danger_fee read over!')
        self.danger_fee_toshop = pd.read_csv('../data/fee/danger_fee_toshop.csv')
        print('危险品补货报价 danger_fee read over!')
        self.danger_fee_rdc = pd.read_csv('../data/fee/danger_fee_rdc.csv')
        dict={'公路':'其他类','恒温运输':'恒温品','危险品运输':'危险品'}
        self.danger_fee_rdc['类型']=self.danger_fee_rdc['运输方式'].map(dict)
        print('rdc补货品类报价 danger_fee_rdc read over!')


    def cal_storage_cost(self, percent_dir=None):
        """
        根据sku数量和仓库计算仓储费

        """
        # 得到所有库存文件
        print()
        print('============开始计算库存成本 {} ==============='.format(self.percent_dir))
        #print(self.percent_dir+'/stock')

        stock_file_path = self.percent_dir+'/stock'
        print('当前计算文件夹：{}'.format(stock_file_path))
        stock_files = [f for f in listdir(stock_file_path) if  'FDC'in f or 'RDC' in f  and isfile(join(stock_file_path, f))]
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
            tmp_stock_df['store_city'] = stock_file.split('_')[-1].split('.')[0]
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
        merged_stock_df = pd.merge(left=stock_all_df, right=self.sku_info_df, left_on='sku_id', right_on='sku_id', how='left')
        print(merged_stock_df.shape)
        if merged_stock_df.shape[0] > stock_all_df.shape[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)

        #print(merged_stock_df)
        print('merge 完成')

        # 判断是否有sku或者仓库信息找不到
        null_sku_df = merged_stock_df.loc[merged_stock_df['sku_id'].isnull() ]
        if null_sku_df.shape[0] > 0:
            print(null_sku_df)
            print('存在sku找不到的情况')
            print('程序退出')
            sys.exit(-1)
        # 计算每个sku的板数
        #第一次合并
        merged_stock_df=merged_stock_df[['date','sku_id','类型','store_city','sku_qty', 'num_preplate']].groupby(['date','sku_id','类型','store_city'],as_index=False).agg(
            {'sku_qty':'sum','num_preplate':'mean'})
        merged_stock_df['plate_num'] = merged_stock_df['sku_qty']/merged_stock_df['num_preplate']
        #print(len(merged_stock_df['sku_qty']==0))
        merged_stock_df['plate_num']=merged_stock_df['plate_num'].apply(lambda x: 0 if np.isinf(x) else x )
        #print(merged_stock_df[['sku_qty','num_preplate','plate_num']])

        #第二次合并
        merged_stock_df_group = merged_stock_df[['date', 'store_city', '类型', 'plate_num']].groupby(
            ['date', 'store_city', '类型'],as_index=False).agg({'plate_num': 'sum'})

        def fun(x):
            xx = math.modf(x)
            if xx[0] == 0:
                return x
            elif xx[0] > 0.5:
                return xx[1] + 1
            elif xx[0] == 0.5:
                return x
            else:
                return xx[1] + 0.5

        merged_stock_df_group['plate_num_int'] = merged_stock_df_group['plate_num'].apply(lambda x: fun(x))
        temp=merged_stock_df_group.shape
        merged_stock_df = pd.merge(left=merged_stock_df_group, right=self.storage_fee_df[['store_city', 'class', '仓租']],
                                   left_on=['store_city', '类型'], right_on=['store_city', 'class'], how='left')
        if merged_stock_df.shape[0]> temp[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        print(merged_stock_df_group.shape)
        # 根据sku类型计算该sku的报价
        merged_stock_df_group['stock_cost_sum'] = merged_stock_df_group['plate_num_int']*merged_stock_df['仓租']
        print('使用板数计算完成')
        merged_stock_df_group.to_csv(r'../data/fee/test/stock_库存仓储.csv')
        #第二次合并
        merged_stock_df_group= merged_stock_df_group[['store_city', '类型','stock_cost_sum']].groupby(['store_city', '类型'],
         as_index=False).agg({'stock_cost_sum': 'sum'})
        print('总价格计算完成')
        merged_stock_df_group=merged_stock_df_group.rename(columns={'store_city':'仓库城市','stock_cost_sum':'仓储费用'})
        print('==================')
        print('库存费最终成本为:{}'.format(merged_stock_df_group['仓储费用'].sum()))
        print('=================库存费 {} OVER ======================'.format(self.percent_dir))

        return  merged_stock_df_group

    def cal_transport_cost_fdc(self, percent_dir=None):
        """
        根据sku数量，出发仓库和目的仓库计算运费
        :param sku_count_df: start_store_id, target_store_id, sku_id, sku_count
        :return:
        """
        # 得到所有补货文件
        print()
        print('============开始计算FDC补货费（运输费和装卸费） {}==========='.format(self.percent_dir))
        replenish_file_path = self.percent_dir+'/repleshment'
        print('当前计算文件夹：{}'.format(replenish_file_path))
        replenish_files_fdc = [f for f in listdir(replenish_file_path) if  'FDC' in f and isfile(join(replenish_file_path, f))]
        print('文件个数====== {}'.format(len(replenish_files_fdc)))
        # 读取补货数据
        replenish_df_list = []
        for i, replenish_file in enumerate(replenish_files_fdc):
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
            tmp_replenish_df['store_city'] = replenish_file.split('_')[-1].split('.')[0]
            shape0 = tmp_replenish_df.shape[0]
            # 删除数量是0的数据
            tmp_replenish_df = tmp_replenish_df.loc[tmp_replenish_df['sku_qty'] > 0]
            print('{}:  {} \t\t{} ------>  {} '.format(i + 1, replenish_file, shape0, tmp_replenish_df.shape[0]))
            # print('删除数量为0的记录后，shape为{}'.format(tmp_replenish_df.shape))
            replenish_df_list.append(tmp_replenish_df)

        # 合并为一个文件
        replenish_all_df = pd.concat(replenish_df_list)
        # merge sku info
        temp1=replenish_all_df.shape
        merged_replenish_df = pd.merge(left=replenish_all_df, right=self.sku_info_df, left_on='sku_id', right_on='sku_id', how='left')
        fdc_df = pd.read_csv(self.percent_dir + '/FDC.csv')
        if replenish_all_df.shape[0]> temp1[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        #print(fdc_df )
        temp2=merged_replenish_df.shape
        merged_replenish_df=pd.merge(merged_replenish_df,fdc_df[['FDC','RDC']],left_on='store_city',right_on='FDC',how='left')
        if merged_replenish_df.shape[0]> temp2[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        #print(merged_replenish_df)
        #拆分危险品与其他商品
        print('补货总数据量为：', merged_replenish_df.shape)
        merged_replenish_df_dangerous=merged_replenish_df[merged_replenish_df['类型']=='危险品']
        merged_replenish_df_common = merged_replenish_df[merged_replenish_df['类型'] != '危险品']
        print('危险品补货数据量为：',merged_replenish_df_dangerous.shape)
        print('普通品补货数据量为：',merged_replenish_df_common.shape)

        # 合并同一天的体积重量
        merged_replenish_df_common_grouped = merged_replenish_df_common[['date','sku_id','类型','RDC','store_city','sku_qty', 'num_preplate']].groupby(['date','sku_id','类型','RDC','store_city'],as_index=False).agg(
            {'sku_qty':'sum','num_preplate':'mean'})
        temp3=merged_replenish_df_common_grouped.shape
        merged_replenish_df_common_grouped = pd.merge(left=merged_replenish_df_common_grouped,right = self.storage_fee_df[['store_city', 'class', '装卸费']],left_on = ['RDC', '类型'],right_on = ['store_city', 'class'], how = 'left')
        if merged_replenish_df_common_grouped.shape[0]> temp3[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        #print(merged_replenish_df_common_grouped.columns)
        temp4=merged_replenish_df_common_grouped.shape
        merged_replenish_df_common_grouped = pd.merge(left=merged_replenish_df_common_grouped, right=self.shipping_fee_df, left_on=['RDC','store_city_x'], right_on=['始发城市','目的城市'], how='left')
        if merged_replenish_df_common_grouped.shape[0]> temp4[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        # 判断是否有sku或者仓库信息找不到
        null_sku_df = merged_replenish_df_common_grouped.loc[merged_replenish_df_common_grouped['装卸费'].isnull()|merged_replenish_df_common_grouped['报价'].isnull()]
        if null_sku_df.shape[0] > 0:
            # print('存在sku或者仓库找不到的情况')
            print('程序退出')
            sys.exit(-1)
        #print(merged_replenish_grouped.columns)
        # merged_replenish_grouped=merged_replenish_grouped.groupby(['date', 'sku_id', '类型','RDC', 'store_city' ,
        # '单位', '报价'],as_index=False).agg({'sku_qty':'sum','num_preplate':'mean', '装卸费':'mean'})

        merged_replenish_df_common_grouped['plate_num'] = merged_replenish_df_common_grouped['sku_qty']/merged_replenish_df_common_grouped['num_preplate']
        merged_replenish_df_common_grouped['plate_num'] = merged_replenish_df_common_grouped['plate_num'].apply(lambda x: 0 if np.isinf(x) else x)
        #第一次合并
        merged_replenish_df_common_grouped = merged_replenish_df_common_grouped[
            ['date', 'RDC', 'store_city_x', 'plate_num', '装卸费', '类型','报价']].groupby(
            ['date', '类型','RDC', 'store_city_x'], as_index=False).agg(
            {'装卸费': 'mean', '报价': 'mean', 'plate_num': 'sum'})


        def fun(x):
            xx = math.modf(x)
            if xx[0] == 0:
                return x
            elif xx[0] > 0.5:
                return xx[1] + 1
            elif xx[0] == 0.5:
                return x
            else:
                return xx[1] + 0.5

        merged_replenish_df_common_grouped['plate_num_int'] = merged_replenish_df_common_grouped['plate_num'].apply(lambda x: fun(x))
        merged_replenish_df_common_grouped['repl_cost_sum'] = merged_replenish_df_common_grouped['plate_num_int'] * merged_replenish_df_common_grouped['报价']
        merged_replenish_df_common_grouped['repl_cost_sum_装卸费'] = merged_replenish_df_common_grouped['plate_num_int'] *  merged_replenish_df_common_grouped['装卸费']

        merged_replenish_df_common_grouped.to_csv(r'../data/fee/test/fdc_replenish_除危险品.csv')
        #第二次合并
        merged_replenish_df_common_grouped = merged_replenish_df_common_grouped[['RDC','store_city_x','类型', 'repl_cost_sum','repl_cost_sum_装卸费']].groupby(['RDC','store_city_x','类型'],as_index=False).agg(
            {'repl_cost_sum': 'sum','repl_cost_sum_装卸费':'sum'})
        #print(merged_replenish_grouped_repl)
        print('fdc普通品补货总价格计算完成')


        ###################################################计算危险品情况
        # 合并同一天的体积重量
        merged_replenish_df_dangerous_grouped = merged_replenish_df_dangerous[
            ['date', 'sku_id', '类型', 'RDC', 'store_city', 'sku_qty', 'num_preplate']].groupby(
            ['date', 'sku_id', '类型', 'RDC', 'store_city'], as_index=False).agg( {'sku_qty': 'sum', 'num_preplate': 'mean'})
        temp5=merged_replenish_df_dangerous_grouped.shape
        merged_replenish_df_dangerous_grouped = pd.merge(left=merged_replenish_df_dangerous_grouped,
                                                         right=self.storage_fee_df[['store_city', 'class', '装卸费']],
                                                         left_on=['RDC', '类型'],
                                                         right_on=['store_city', 'class'], how='left')
        if merged_replenish_df_dangerous_grouped.shape[0]> temp5[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        temp6=merged_replenish_df_dangerous_grouped.shape
        merged_replenish_df_dangerous_grouped = pd.merge(left=merged_replenish_df_dangerous_grouped,
                                                      right=self.danger_fee_new, left_on=['RDC', 'store_city_x','类型'],
                                                      right_on=['起始地', '目的地','类型'], how='left')
        if merged_replenish_df_dangerous_grouped.shape[0]> temp6[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)

        # 判断是否有sku或者仓库信息找不到
        null_danger_sku_df = merged_replenish_df_dangerous_grouped.loc[
            merged_replenish_df_dangerous_grouped['装卸费'].isnull() | merged_replenish_df_dangerous_grouped['含税报价'].isnull()]
        if null_danger_sku_df.shape[0] > 0:
            # print('存在sku或者仓库找不到的情况')
            print('程序退出')
            sys.exit(-1)
        #第一次合并
        merged_replenish_df_dangerous_grouped = merged_replenish_df_dangerous_grouped[
            ['date', 'sku_id', '类型','RDC', 'store_city_x', 'sku_qty', 'num_preplate','装卸费','含税报价']].groupby(
            ['date', 'sku_id', '类型','RDC', 'store_city_x'], as_index=False).agg({'sku_qty': 'sum', 'num_preplate': 'mean','装卸费': 'mean','含税报价': 'mean'})

        merged_replenish_df_dangerous_grouped['plate_num'] = merged_replenish_df_dangerous_grouped['sku_qty'] / merged_replenish_df_dangerous_grouped['num_preplate']
        merged_replenish_df_dangerous_grouped['plate_num'] = merged_replenish_df_dangerous_grouped['plate_num'].apply(  lambda x: 0 if np.isinf(x) else x)
        #第二次合并
        merged_replenish_df_dangerous_grouped = merged_replenish_df_dangerous_grouped[
            ['date', 'RDC', 'store_city_x', 'plate_num', '装卸费', '类型', '含税报价']].groupby(
            ['date', '类型', 'RDC', 'store_city_x'], as_index=False).agg(
            {'装卸费': 'mean', '含税报价': 'mean', 'plate_num': 'sum'})

        merged_replenish_df_dangerous_grouped['plate_num_int'] = merged_replenish_df_dangerous_grouped['plate_num'].apply(lambda x: fun(x))
        merged_replenish_df_dangerous_grouped['repl_cost_sum'] = merged_replenish_df_dangerous_grouped['plate_num_int'] * \
                                                                 merged_replenish_df_dangerous_grouped['含税报价']
        merged_replenish_df_dangerous_grouped['repl_cost_sum_装卸费'] = merged_replenish_df_dangerous_grouped['plate_num_int'] * \
                                                                     merged_replenish_df_dangerous_grouped['装卸费']
        merged_replenish_df_dangerous_grouped.to_csv(r'../data/fee/test/fdc_replenish_危险品.csv')
        #第三次合并
        merged_replenish_df_dangerous_grouped = merged_replenish_df_dangerous_grouped[
            [ 'RDC','store_city_x','类型', 'repl_cost_sum', 'repl_cost_sum_装卸费']].groupby(['RDC','store_city_x','类型'], as_index=False).agg(
            {'repl_cost_sum': 'sum', 'repl_cost_sum_装卸费': 'sum'})
        print('危险品补货总价格计算完成')
        #合并结果
        merged_replenish_grouped_repl_comb=pd.concat([merged_replenish_df_common_grouped,merged_replenish_df_dangerous_grouped],axis=0)
        merged_replenish_grouped_repl_comb = merged_replenish_grouped_repl_comb.rename(
            columns={'store_city_x': '仓库城市', 'repl_cost_sum': '补货运输费用', 'repl_cost_sum_装卸费': '装卸费用'})

        print(merged_replenish_grouped_repl_comb)
        print('补货运费最终为:{}'.format(merged_replenish_grouped_repl_comb['补货运输费用'].sum()))
        print('装卸费用最终为:{}'.format(merged_replenish_grouped_repl_comb['装卸费用'].sum()))
        print('=================补货运输费/装卸货运费 {} OVER ======================'.format(self.percent_dir))
        return (merged_replenish_grouped_repl_comb)


    def cal_transport_cost_rdc(self, percent_dir=None):
        """
        根据sku数量，出发仓库和目的仓库计算运费
        :param sku_count_df: start_store_id, target_store_id, sku_id, sku_count
        :return:
        """
        # 得到所有补货文件
        print()
        print('============开始计算RDC补货费（运输费和装卸费） {}==========='.format(self.percent_dir))
        replenish_file_path = self.percent_dir+'/repleshment'
        print('当前计算文件夹：{}'.format(replenish_file_path))
        replenish_files_fdc = [f for f in listdir(replenish_file_path) if  'RDC' in f and isfile(join(replenish_file_path, f))]
        print('文件个数====== {}'.format(len(replenish_files_fdc)))
        # 读取补货数据
        replenish_df_list = []
        for i, replenish_file in enumerate(replenish_files_fdc):
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
            tmp_replenish_df['store_city'] = replenish_file.split('_')[-1].split('.')[0]
            shape0 = tmp_replenish_df.shape[0]
            # 删除数量是0的数据
            tmp_replenish_df = tmp_replenish_df.loc[tmp_replenish_df['sku_qty'] > 0]
            print('{}:  {} \t\t{} ------>  {} '.format(i + 1, replenish_file, shape0, tmp_replenish_df.shape[0]))
            # print('删除数量为0的记录后，shape为{}'.format(tmp_replenish_df.shape))
            replenish_df_list.append(tmp_replenish_df)

        # 合并为一个文件
        replenish_all_df = pd.concat(replenish_df_list)
        # merge sku info
        temp7=replenish_all_df.shape
        merged_replenish_df = pd.merge(left=replenish_all_df, right=self.sku_info_df[['sku_id','num_preplate','类型']], left_on='sku_id', right_on='sku_id', how='left')
        if merged_replenish_df.shape[0]> temp7[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        # fdc_df = pd.read_csv(self.percent_dir+'/RDC.csv')
        #print(fdc_df )
        #merged_replenish_df=pd.merge(merged_replenish_df,fdc_df,left_on='store_city',right_on='RDC',how='left')
        #print(merged_replenish_df)
        #拆分危险品与其他商品
        print('rdc补货数据量为：',merged_replenish_df.shape)
        merged_replenish_df_dangerous=merged_replenish_df[merged_replenish_df['类型']=='危险品']
        merged_replenish_df_own = merged_replenish_df[merged_replenish_df['类型'] == '外购品']
        merged_replenish_df_xs = merged_replenish_df[merged_replenish_df['类型'] == 'XS饮料']
        #print(merged_replenish_df_xs)
        merged_replenish_df_common = merged_replenish_df[merged_replenish_df['类型'].isin(['自有品','面膜','恒温品'])]

        # a=merged_replenish_df[(merged_replenish_df['类型'] == '自有品') ]
        # b=merged_replenish_df[(merged_replenish_df['类型'] == '面膜')]
        # c=merged_replenish_df[(merged_replenish_df['类型'] == '恒温品')]
        # d=a.append(b)
        # merged_replenish_df_common = d.append(c)
        # merged_replenish_df_common = merged_replenish_df[(merged_replenish_df['类型'] == '自有品')|(merged_replenish_df['类型'] == '面膜')|(merged_replenish_df['类型'] == '恒温品')]
        print('危险品补货数据量为：',merged_replenish_df_dangerous.shape)
        print('外购品补货数据量为：', merged_replenish_df_own.shape)
        print('XS饮料补货数据量为：', merged_replenish_df_xs.shape)
        print('普通品补货数据量为：',merged_replenish_df_common.shape)
        # print(a.shape)
        # print(b.shape)
        # print(c.shape)
        # print(d.shape)


        # 计算除危险品外从广州工厂补货到RDC费用
        merged_replenish_df_common['总仓']='广州'
        merged_replenish_df_common['类型']=merged_replenish_df_common['类型'].apply(lambda x:'其他类'if x!='恒温品' else x)
        temp8=merged_replenish_df_common.shape
        merged_replenish_grouped = pd.merge(left=merged_replenish_df_common, right=self.danger_fee_rdc, left_on=['总仓','store_city','类型'], right_on=['起始地（城）','目的地（城市）','类型'], how='left')
        if merged_replenish_grouped.shape[0] > temp8[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        # merged_replenish_grouped = merged_replenish_grouped[
        #     ['date', 'sku_id', '总仓','store_city', 'sku_qty', 'num_preplate', '含税报价','类型']].groupby(
        #     ['date', 'sku_id', '总仓',  '类型','store_city'], as_index=False).agg(
        #     {'sku_qty': 'sum', 'num_preplate': 'mean','含税报价':'mean'})
        merged_replenish_grouped['含税报价']=merged_replenish_grouped['含税报价'].fillna(0)


        # # 判断是否有sku或者仓库信息找不到
        # null_sku_df = merged_replenish_grouped.loc[merged_replenish_grouped['含税报价'].isnull()]
        # if null_sku_df.shape[0] > 0:
        #     # print('存在sku或者仓库找不到的情况')
        #     print(null_sku_df['store_city'])
        #     print('程序退出')
        #     sys.exit(-1)
        # #print(merged_replenish_grouped.columns)


        merged_replenish_grouped['plate_num'] = merged_replenish_grouped['sku_qty']/merged_replenish_grouped['num_preplate']
        merged_replenish_grouped['plate_num'] = merged_replenish_grouped['plate_num'].apply(lambda x: 0 if np.isinf(x) else x)


        merged_replenish_grouped = merged_replenish_grouped.groupby(['date','类型', 'store_city'], as_index=False).agg(
            {'plate_num': 'sum',  '含税报价': 'mean'})


        def fun(x):
            xx = math.modf(x)
            if xx[0] == 0:
                return x
            elif xx[0] > 0.5:
                return xx[1] + 1
            elif xx[0] == 0.5:
                return x
            else:
                return xx[1] + 0.5

        merged_replenish_grouped['plate_num_int'] = merged_replenish_grouped['plate_num'].apply(lambda x: fun(x))
        merged_replenish_grouped['repl_cost_sum'] = merged_replenish_grouped['plate_num_int'] * merged_replenish_grouped['含税报价']
        #print(merged_replenish_grouped)
        merged_replenish_grouped.to_csv(r'../data/fee/test/rdc_replenish_普通品.csv')
        merged_replenish_grouped_repl = merged_replenish_grouped[['store_city', 'repl_cost_sum']].groupby('store_city',as_index=False).agg(
            {'repl_cost_sum': 'sum'})
        print('普通品rdc补货总价格',merged_replenish_grouped_repl['repl_cost_sum'].sum())
        print('普通品rdc补货总价格计算完成')

        # 计算XS饮料从武汉工厂补货到RDC费用
        merged_replenish_df_xs_new=merged_replenish_df_xs.copy()
        merged_replenish_df_xs_new['总仓'] = '武汉'
        #merged_replenish_df_xs_new['类型']='其他类'
        temp9=merged_replenish_df_xs_new.shape
        merged_replenish_df_xs_grouped = pd.merge(left=merged_replenish_df_xs_new, right=self.shipping_fee_df,
                                            left_on=['总仓', 'store_city'], right_on=['始发城市', '目的城市'],
                                            how='left')
        if merged_replenish_df_xs_grouped.shape[0] > temp9[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        # merged_replenish_df_xs_grouped = merged_replenish_df_xs_grouped[
        #     ['date', 'sku_id', '总仓', 'store_city', 'sku_qty', 'num_preplate','含税报价', '类型']].groupby(
        #     ['date', 'sku_id', '总仓', '类型' ,'store_city'], as_index=False).agg(
        #     {'sku_qty': 'sum', 'num_preplate': 'mean','含税报价':'mean'})
        merged_replenish_df_xs_grouped['报价']=merged_replenish_df_xs_grouped['报价'].fillna(200)

        # 判断是否有sku或者仓库信息找不到
        null_sku_df = merged_replenish_df_xs_grouped.loc[merged_replenish_df_xs_grouped['报价'].isnull()]
        if null_sku_df.shape[0] > 0:
            # print('存在sku或者仓库找不到的情况')
            print(null_sku_df)
            print('程序退出')
            sys.exit(-1)
        # print(merged_replenish_grouped.columns)


        merged_replenish_df_xs_grouped['plate_num'] = merged_replenish_df_xs_grouped['sku_qty'] / merged_replenish_df_xs_grouped[ 'num_preplate']
        merged_replenish_df_xs_grouped['plate_num'] = merged_replenish_df_xs_grouped['plate_num'].apply(
            lambda x: 0 if np.isinf(x) else x)

        merged_replenish_df_xs_grouped = merged_replenish_df_xs_grouped.groupby(['date', '类型', 'store_city'], as_index=False).agg(
            {'plate_num': 'sum', '报价': 'mean'})

        merged_replenish_df_xs_grouped['plate_num_int'] = merged_replenish_df_xs_grouped['plate_num'].apply(lambda x: fun(x))

        merged_replenish_df_xs_grouped['repl_cost_sum'] = merged_replenish_df_xs_grouped['plate_num_int'] * \
                                                          merged_replenish_df_xs_grouped['报价']
        #merged_replenish_df_xs_grouped['repl_cost_sum_短驳'] = merged_replenish_df_xs_grouped['plate_num_int'] * 80
        # print(merged_replenish_grouped)
        merged_replenish_df_xs_grouped.to_csv(r'../data/fee/test/rdc_replenish_XS饮料.csv')
        merged_replenish_df_xs_grouped = merged_replenish_df_xs_grouped[['store_city', 'repl_cost_sum']].groupby('store_city',as_index=False).agg(
            {'repl_cost_sum': 'sum'})
        print('XS饮料rdc补货总价格',merged_replenish_df_xs_grouped['repl_cost_sum'].sum())
        print('XS饮料rdc补货总价格计算完成')

        ########计算危险品情况   危险品从工厂到总仓费用
        # merged_replenish_df_dangerous=merged_replenish_df_dangerous[['date', 'sku_id','store_city', 'sku_qty', 'num_preplate', '类型']].groupby(
        #     ['date', 'sku_id', '类型', 'store_city'], as_index=False).agg(
        #     {'sku_qty': 'sum', 'num_preplate': 'mean'})
        merged_replenish_df_dangerous['plate_num'] = merged_replenish_df_dangerous['sku_qty'] / merged_replenish_df_dangerous[
            'num_preplate']
        merged_replenish_df_dangerous['plate_num'] = merged_replenish_df_dangerous['plate_num'].apply(
            lambda x: 0 if np.isinf(x) else x)
        merged_replenish_df_dangerous = merged_replenish_df_dangerous.groupby(['date', '类型', 'store_city'], as_index=False).agg(
           {'plate_num': 'sum'})
        merged_replenish_df_dangerous['plate_num_int'] = merged_replenish_df_dangerous['plate_num'].apply(lambda x: fun(x))
        merged_replenish_df_dangerous['repl_cost_sum'] = merged_replenish_df_dangerous['plate_num_int'] * 700
        #merged_replenish_df_dangerous['repl_cost_sum_短驳'] = merged_replenish_df_dangerous['plate_num_int'] * 200
        merged_replenish_df_dangerous.to_csv(r'../data/fee/test/rdc_replenish_危险品.csv')
        merged_replenish_df_dangerous = merged_replenish_df_dangerous[['store_city', 'repl_cost_sum']].groupby('store_city',as_index=False).agg(
            {'repl_cost_sum': 'sum'})
        print( '危险品工厂补货总价格',merged_replenish_df_dangerous['repl_cost_sum'].sum())
        print('危险品工厂补货总价格计算完成')


        #######################计算cdc仓到rdc的费用
        # stock_file_path = self.percent_dir + '/stock'
        # print('当前计算文件夹：{}'.format(stock_file_path))
        # stock_files = [f for f in listdir(stock_file_path) if
        #                'CDC'  in f and isfile(join(stock_file_path, f))]
        # print('文件个数:{}'.format(len(stock_files)))
        # # 读取库存数据
        # stock_df_list = []
        # for i, stock_file in enumerate(stock_files):
        #     tmp_stock_df = pd.read_csv(join(stock_file_path, stock_file),
        #                                encoding='utf-8',
        #                                engine='python',
        #                                sep=',',
        #                                dtype={0: np.str_}
        #                                )
        #     tmp_stock_df.rename(columns={tmp_stock_df.columns[0]: 'date'}, inplace=True)
        #     # 转换成三元组形式
        #     tmp_stock_df = pd.melt(tmp_stock_df,
        #                            id_vars='date',
        #                            value_vars=list(tmp_stock_df.columns[1:]),  # list of days of the week
        #                            var_name='sku_id',
        #                            value_name='sku_qty')
        #     # 增加仓库名称
        #     tmp_stock_df['store_city'] = stock_file.split('_')[-1].split('.')[0]
        #     shape0 = tmp_stock_df.shape[0]
        #     # print('{}读取完成，shape{}'.format(stock_file, tmp_stock_df.shape))
        #     # 删除数量是0的数据
        #     tmp_stock_df = tmp_stock_df.loc[tmp_stock_df['sku_qty'] > 0]
        #
        #     print('{}:  {} \t\t{} ------>  {} '.format(i + 1, stock_file, shape0, tmp_stock_df.shape[0]))
        #     stock_df_list.append(tmp_stock_df)
        #
        # # 合并为一个文件
        # stock_all_df = pd.concat(stock_df_list)
        # print('合并完成，总数据量{}'.format(stock_all_df.shape))
        # # 合并为一个文件
        # replenish_all_df_cdc = pd.merge(left=stock_all_df, right=self.sku_info_df[['sku_id','num_preplate', '类型']],
        #                                left_on='sku_id', right_on='sku_id', how='left')
        replenish_all_df_cdc=pd.concat([merged_replenish_df_own])
        replenish_all_df_cdc['总仓']=replenish_all_df_cdc['类型'].apply(lambda x:'武汉'if x=='XS饮料'else '广州')
        # replenish_all_df_cdc = replenish_all_df_cdc[
        #     ['date', 'sku_id', '总仓', 'store_city', 'sku_qty', 'num_preplate']].groupby(
        #     ['date', 'sku_id', '总仓',  'store_city'], as_index=False).agg(
        #     {'sku_qty': 'sum', 'num_preplate': 'mean'})
        temp10=replenish_all_df_cdc.shape
        replenish_all_df_cdc = pd.merge(left=replenish_all_df_cdc,
                                        right=self.storage_fee_df[['store_city', 'class', '装卸费']],
                                        left_on=['总仓', '类型'],
                                        right_on=['store_city', 'class'], how='left')
        if replenish_all_df_cdc.shape[0] > temp10[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        #print(replenish_all_df_cdc.columns)
        temp11 = replenish_all_df_cdc.shape
        replenish_all_df_cdc = pd.merge(left=replenish_all_df_cdc, right=self.shipping_fee_df,
                                            left_on=['总仓', 'store_city_x'], right_on=['始发城市', '目的城市'],
                                            how='left')
        if replenish_all_df_cdc.shape[0] > temp11[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        # print(replenish_all_df_cdc.columns)
        # print(replenish_all_df_cdc)


        # # 判断是否有sku或者仓库信息找不到
        # null_sku_df = replenish_all_df_cdc.loc[merged_replenish_grouped['含税报价'].isnull()]
        # if null_sku_df.shape[0] > 0:
        #     # print('存在sku或者仓库找不到的情况')
        #     print('程序退出')
        #     sys.exit(-1)
        # # print(merged_replenish_grouped.columns)

        replenish_all_df_cdc['plate_num'] = replenish_all_df_cdc['sku_qty'] / replenish_all_df_cdc[
            'num_preplate']
        replenish_all_df_cdc['plate_num'] = replenish_all_df_cdc['plate_num'].apply(
            lambda x: 0 if np.isinf(x) else x)

        replenish_all_df_cdc = replenish_all_df_cdc[['date','store_city_x','plate_num','报价','装卸费']].groupby(
            ['date','store_city_x'], as_index=False).agg(
            {'plate_num': 'sum','报价': 'mean','装卸费': 'mean'})

        replenish_all_df_cdc['plate_num_int'] = replenish_all_df_cdc['plate_num'].apply(lambda x: fun(x))
        replenish_all_df_cdc['repl_cost_sum'] = replenish_all_df_cdc['plate_num_int'] * \
                                                replenish_all_df_cdc['报价']

        replenish_all_df_cdc['repl_cost_sum_装卸费'] = replenish_all_df_cdc['plate_num_int'] * \
                                                replenish_all_df_cdc['装卸费']
        replenish_all_df_cdc.to_csv(r'../data/fee/test/rdc_replenish_cdc外购品.csv')
        replenish_all_df_cdc = replenish_all_df_cdc[['store_city_x',  'repl_cost_sum','repl_cost_sum_装卸费']].groupby(['store_city_x'], as_index=False).agg(
            {'repl_cost_sum': 'sum','repl_cost_sum_装卸费':'sum'})
        #print(replenish_all_df_cdc)
        print('cdc补货装卸总价格:',replenish_all_df_cdc['repl_cost_sum'].sum()+replenish_all_df_cdc['repl_cost_sum_装卸费'].sum())
        print('cdc补货分拣总价格计算完成')
        # merged_replenish_df_dangerous = merged_replenish_df_dangerous.rename(
        #     columns={'store_city': '仓库城市', 'repl_cost_sum': '补货运输费用', 'repl_cost_sum_装卸费': '装卸费用'})
        # merged_replenish_grouped_repl_comb = pd.concat(merged_replenish_grouped_repl, merged_replenish_df_dangerous)
        #
        # print(merged_replenish_grouped_repl_comb)
        # print('补货运费最终为:{}'.format(merged_replenish_grouped_repl['补货运输费用'].sum()))
        # print('进仓费最终为:{}'.format(merged_replenish_grouped_repl['装卸费用'].sum()))
        print('=================进仓费/补货运费 {} OVER ======================'.format(self.percent_dir))
        return (merged_replenish_grouped_repl,merged_replenish_df_xs_grouped,merged_replenish_df_dangerous,replenish_all_df_cdc)




    def cal_delivery_toB_cost(self,sale_dir1=None):

        print()
        print('============开始计算门店补货费（运输费和装卸费） {}==========='.format(self.percent_dir))
        replenish_file_path = self.percent_dir + '/repleshment'
        print('当前计算文件夹：{}'.format(replenish_file_path))
        replenish_files_shop = [f for f in listdir(replenish_file_path) if
                               'shop' in f and isfile(join(replenish_file_path, f))]
        print('文件个数====== {}'.format(len(replenish_files_shop)))
        # 读取补货数据
        replenish_df_list = []
        for i, replenish_file in enumerate(replenish_files_shop):
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
            tmp_replenish_df['store_city'] = replenish_file.split('_')[-1].split('.')[0]
            shape0 = tmp_replenish_df.shape[0]
            # 删除数量是0的数据
            tmp_replenish_df = tmp_replenish_df.loc[tmp_replenish_df['sku_qty'] > 0]
            print('{}:  {} \t\t{} ------>  {} '.format(i + 1, replenish_file, shape0, tmp_replenish_df.shape[0]))
            # print('删除数量为0的记录后，shape为{}'.format(tmp_replenish_df.shape))
            replenish_df_list.append(tmp_replenish_df)

        # 合并为一个文件
        replenish_all_df = pd.concat(replenish_df_list)
        # merge sku info
        temp12=replenish_all_df.shape
        merged_replenish_shop_df = pd.merge(left=replenish_all_df, right=self.sku_info_df, left_on='sku_id',
                                       right_on='sku_id', how='left')
        if merged_replenish_shop_df.shape[0] > temp12[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        shop_df = pd.read_csv(self.percent_dir + '/shops_match.csv')

        # print(fdc_df )
        print('shopmatch数据量为：',shop_df.shape)
        shop_df=shop_df[['shop_id','RDC','end_city']]
        shop_df=shop_df.drop_duplicates()
        print('删除重复项后数据量为：', shop_df.shape)
        temp13=merged_replenish_shop_df.shape
        merged_replenish_shop_df = pd.merge(merged_replenish_shop_df, shop_df, left_on='store_city', right_on='shop_id', how='left')
        if merged_replenish_shop_df.shape[0] > temp13[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        # print(merged_replenish_df)
        # 拆分危险品与其他商品
        print('tob商家补货数据量为：', merged_replenish_shop_df.shape)
        merged_replenish_shop_df_dangerous = merged_replenish_shop_df[merged_replenish_shop_df['类型'] == '危险品']
        merged_replenish_shop_df_common = merged_replenish_shop_df[merged_replenish_shop_df['类型'] != '危险品']
        print('危险品补货数据量为：', merged_replenish_shop_df_dangerous.shape)
        print('普通品补货数据量为：', merged_replenish_shop_df_common.shape)
        ################################计算普通品
        # 合并同一天的体积重量
        # merged_replenish_shop_df_common_grouped = pd.merge(left=merged_replenish_shop_df_common,
        #                                     right=self.storage_fee_df[['store_city', '装卸费']], left_on='RDC',
        #                                     right_on='store_city', how='left')
        # merged_replenish_shop_df_common_grouped = pd.merge(left=merged_replenish_shop_df_common_grouped, right=self.shipping_fee_df[['始发城市', '目的城市','报价']],
        #                                     left_on=['RDC', 'end_city'], right_on=['始发城市', '目的城市'], how='left',indicator=True)
        #print(merged_replenish_grouped.columns)
        #第一次合并数据
        merged_replenish_shop_df_common_grouped = merged_replenish_shop_df_common[
            ['date', 'sku_id', 'RDC', 'store_city','类型' ,'end_city', 'sku_qty', 'num_preplate']].groupby(
            ['date', 'sku_id', 'RDC', 'store_city', 'end_city','类型' ], as_index=False).agg(
            {'sku_qty': 'sum', 'num_preplate': 'mean'})

        #print(merged_replenish_grouped['_merge'].describe())
        #merged_replenish_grouped.fillna(merged_replenish_grouped.mean()['报价'])

        #print(merged_replenish_grouped['报价'])
        #merged_replenish_grouped['报价']=merged_replenish_grouped['报价'].apply(lambda x: np.mean(x) if np.isnan(x) else x)

        merged_replenish_shop_df_common_grouped['plate_num'] = merged_replenish_shop_df_common_grouped['sku_qty'] / merged_replenish_shop_df_common_grouped['num_preplate']
        merged_replenish_shop_df_common_grouped['plate_num'] = merged_replenish_shop_df_common_grouped['plate_num'].apply(
            lambda x: 0 if np.isinf(x) else x)
        #第二次合并，除去sku
        merged_replenish_shop_df_common_grouped=merged_replenish_shop_df_common_grouped[['date', 'RDC', 'store_city', 'end_city' ,'plate_num','类型']].groupby(
            ['date', 'RDC', 'store_city', '类型','end_city' ],as_index=False).agg({'plate_num':'sum'})


        def fun(x):
            xx = math.modf(x)
            if xx[0] == 0:
                return x
            elif xx[0] > 0.5:
                return xx[1] + 1
            elif xx[0] == 0.5:
                return x
            else:
                return xx[1] + 0.5

        merged_replenish_shop_df_common_grouped['plate_num_int'] = merged_replenish_shop_df_common_grouped['plate_num'].apply(lambda x: fun(x))
        temp14=merged_replenish_shop_df_common_grouped.shape
        merged_replenish_shop_df_common_grouped = pd.merge(left=merged_replenish_shop_df_common_grouped,
                                                           right=self.storage_fee_df[['store_city','class', '装卸费']],
                                                           left_on=['RDC','类型'],
                                                           right_on=['store_city','class'], how='left')
        if merged_replenish_shop_df_common_grouped.shape[0] > temp14[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        temp15=merged_replenish_shop_df_common_grouped.shape
        merged_replenish_shop_df_common_grouped = pd.merge(left=merged_replenish_shop_df_common_grouped,
                                                           right=self.shipping_fee_df[['始发城市', '目的城市', '报价']],
                                                           left_on=['RDC', 'end_city'], right_on=['始发城市', '目的城市'],
                                                           how='left')
        if merged_replenish_shop_df_common_grouped.shape[0] > temp15[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        merged_replenish_shop_df_common_grouped['报价'] = merged_replenish_shop_df_common_grouped['报价'].fillna(150)

        merged_replenish_shop_df_common_grouped['repl_cost_sum'] = merged_replenish_shop_df_common_grouped['plate_num_int'] *  merged_replenish_shop_df_common_grouped['报价']
        merged_replenish_shop_df_common_grouped['repl_cost_sum_装卸费'] = merged_replenish_shop_df_common_grouped['plate_num_int'] * merged_replenish_shop_df_common_grouped['装卸费']

        merged_replenish_shop_df_common_grouped.to_csv(r'../data/fee/test/tob_replenish_除危险品.csv')
        #第三次合并，除去日期
        merged_replenish_shop_df_common_grouped = merged_replenish_shop_df_common_grouped[
            ['RDC','store_city_x','end_city','类型' ,'repl_cost_sum', 'repl_cost_sum_装卸费']].groupby(['RDC','end_city','store_city_x','类型'], as_index=False).agg(
            {'repl_cost_sum': 'sum', 'repl_cost_sum_装卸费': 'sum'})

        ################################################计算危险品
        # 合并同一天的体积重量
        temp16=merged_replenish_shop_df_dangerous.shape
        merged_replenish_shop_df_dangerous_grouped = pd.merge(left=merged_replenish_shop_df_dangerous,
                                                              right=self.storage_fee_df[['store_city', 'class', '装卸费']],
                                                              left_on=['RDC', '类型'],
                                                              right_on=['store_city', 'class'], how='left')
        if merged_replenish_shop_df_dangerous_grouped.shape[0] > temp16[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        temp17=merged_replenish_shop_df_dangerous_grouped.shape
        merged_replenish_shop_df_dangerous_grouped = pd.merge(left=merged_replenish_shop_df_dangerous_grouped,
                                                           right=self.danger_fee_toshop[['RDC', 'end_city', 'fee']],
                                                           left_on=['RDC', 'end_city'], right_on=['RDC', 'end_city'],
                                                           how='left', indicator=True)
        if merged_replenish_shop_df_dangerous_grouped.shape[0] > temp17[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        #print(merged_replenish_shop_df_dangerous_grouped.columns)
        # 第一次合并数据
        merged_replenish_shop_df_dangerous_grouped = merged_replenish_shop_df_dangerous_grouped[
            ['date', 'sku_id', 'RDC', 'store_city_x', '类型', 'end_city', 'sku_qty', 'num_preplate', '装卸费', 'fee']].groupby(
            ['date', 'sku_id', 'RDC','类型', 'store_city_x', 'end_city'], as_index=False).agg({'sku_qty': 'sum', 'num_preplate': 'mean', '装卸费': 'mean', 'fee': 'mean'})

        merged_replenish_shop_df_dangerous_grouped['plate_num'] = merged_replenish_shop_df_dangerous_grouped['sku_qty'] /  merged_replenish_shop_df_dangerous_grouped[  'num_preplate']
        merged_replenish_shop_df_dangerous_grouped['plate_num'] = merged_replenish_shop_df_dangerous_grouped['plate_num'].apply(
            lambda x: 0 if np.isinf(x) else x)


        # merged_replenish_df_dangerous_group = merged_replenish_df_dangerous_group[
        #     ['date', 'sku_id', '类型', 'RDC', 'store_city_x', 'end_city', 'sku_qty', 'num_preplate', '装卸费',
        #      'fee']].groupby(
        #     ['date', 'sku_id', '类型', 'RDC', 'store_city_x', 'end_city'], as_index=False).agg(
        #     {'sku_qty': 'sum', 'num_preplate': 'mean', '装卸费': 'mean', 'fee': 'mean'})

        # 判断是否有sku或者仓库信息找不到
        null_sku_df = merged_replenish_shop_df_dangerous_grouped.loc[
            merged_replenish_shop_df_dangerous_grouped['装卸费'].isnull() | merged_replenish_shop_df_dangerous_grouped['fee'].isnull()]
        if null_sku_df.shape[0] > 0:
            # print('存在sku或者仓库找不到的情况')
            #print(null_sku_df)
            print('程序退出')
            sys.exit(-1)
        # print(merged_replenish_grouped.columns)
        # #merged_replenish_grouped = merged_replenish_grouped.groupby(['date', 'sku_id', 'store_city',
        #      'end_city'], as_index=False).agg(
        #     {'sku_qty': 'sum', 'num_preplate': 'mean', '装卸费': 'mean', '报价':'mean'})

        # merged_replenish_df_dangerous_group = merged_replenish_df_dangerous_group[
        #     ['date', 'RDC', 'store_city_x', 'end_city', 'plate_num', '类型','装卸费', 'fee']].groupby(
        #     ['date', 'RDC', 'store_city_x', 'end_city','类型'], as_index=False).agg(
        #     {'装卸费': 'mean', 'fee': 'mean', 'plate_num': 'sum'})

        merged_replenish_shop_df_dangerous_grouped['plate_num_int'] = merged_replenish_shop_df_dangerous_grouped['plate_num'].apply(lambda x: fun(x))
        merged_replenish_shop_df_dangerous_grouped['repl_cost_sum'] = merged_replenish_shop_df_dangerous_grouped['plate_num_int'] * \
                                                                      merged_replenish_shop_df_dangerous_grouped['fee']
        merged_replenish_shop_df_dangerous_grouped['repl_cost_sum_装卸费'] = merged_replenish_shop_df_dangerous_grouped['plate_num_int'] * \
                                                                          merged_replenish_shop_df_dangerous_grouped['装卸费']
        #第二次数据合并
        merged_replenish_shop_df_dangerous_grouped = merged_replenish_shop_df_dangerous_grouped[
            ['date', 'RDC', 'store_city_x', '类型', 'end_city',  'plate_num_int', 'repl_cost_sum','repl_cost_sum_装卸费']].groupby(
            ['date', 'RDC', 'store_city_x', '类型', 'end_city'], as_index=False).agg(
            {'plate_num_int': 'sum', 'repl_cost_sum': 'sum', 'repl_cost_sum_装卸费': 'sum'})
        merged_replenish_shop_df_dangerous_grouped.to_csv(r'../data/fee/test/tob_replenish_危险品.csv')
        #第三次合并
        merged_replenish_shop_df_dangerous_grouped = merged_replenish_shop_df_dangerous_grouped[
            ['RDC','store_city_x','end_city','类型', 'repl_cost_sum', 'repl_cost_sum_装卸费']].groupby(['RDC','store_city_x','类型','end_city'], as_index=False).agg(
            {'repl_cost_sum': 'sum', 'repl_cost_sum_装卸费': 'sum'})
        ##############合并结果
        merged_replenish_grouped_repl_comb=pd.concat([merged_replenish_shop_df_common_grouped,merged_replenish_shop_df_dangerous_grouped])
        # print(merged_replenish_grouped_repl)
        print(merged_replenish_grouped_repl_comb)
        print('补货总价格计算完成')
        merged_replenish_grouped_repl_comb = merged_replenish_grouped_repl_comb.rename(
            columns={'store_city_x': '商铺编码', 'repl_cost_sum': '补货运输费用', 'repl_cost_sum_装卸费': '装卸费用'})

        print(merged_replenish_grouped_repl_comb)
        print('补货运费最终为:{}'.format(merged_replenish_grouped_repl_comb['补货运输费用'].sum()))
        print('装卸费用最终为:{}'.format(merged_replenish_grouped_repl_comb['装卸费用'].sum()))
        print('=================补货运输费/装卸费 {} OVER ======================'.format(self.percent_dir))
        return (merged_replenish_grouped_repl_comb)


    def cal_delivery_toC_cost(self,sale_dir1=None):

        toc_data = pd.read_csv(sale_dir1, usecols=['id', 'date', 'sku_id', 'qty', 'start_city', 'end_city'])

        print('sale_toc read over!')
        toc_data['sku_id'] = toc_data['sku_id'].apply(lambda x: str(x).strip())
        # merge sku info
        temp17=toc_data.shape
        merged_toc_df = pd.merge(left=toc_data, right=self.sku_info_df[['sku_id', '类型', 'weight']], left_on='sku_id',
                                 right_on='sku_id',
                                 how='left')
        if merged_toc_df.shape[0] > temp17[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        print('sku_info_df 合并完成')
        print('配送数据量为：', merged_toc_df.shape)

        # 合并同一订单的体积重量
        merged_toc_df['weight_sum']=merged_toc_df['weight']*merged_toc_df['qty']
        merged_toc_df_grouped = merged_toc_df.groupby(
            ['date', 'id',  'start_city', 'end_city'], as_index=False).agg(
            {'weight_sum': 'sum'})
        print(len(merged_toc_df_grouped))


        # #merged_toc_df_grouped = pd.merge(left=merged_toc_df_grouped,
        #                                     right=self.toc_fee_new[['始发地','目的城市','首重（元/1KG）', '续重（元/0.5KG）']], left_on=['start_city', 'end_city'],
        #                                     right_on=['始发地','目的城市'], how='left')
        merged_toc_df_grouped['配送费']=merged_toc_df_grouped['weight_sum'].apply(lambda x:8 if x<1 else 8+(x-1)*0.7)
        merged_toc_df_grouped['分拣费']=14.31
        merged_toc_df_grouped.to_csv(r'../data/fee/test/toc_配送分拣.csv')

        merged_toc_df_grouped=merged_toc_df_grouped[['start_city','配送费','分拣费']].groupby('start_city').agg(sum)

        print(merged_toc_df_grouped)
        print('分拣费最终为:{}'.format(merged_toc_df_grouped['分拣费'].sum()))
        print('配送费最终为:{}'.format(merged_toc_df_grouped['配送费'].sum()))
        print('=================分拣费/配送费 {} OVER ======================'.format(self.percent_dir))
        return (merged_toc_df_grouped)





if __name__ == '__main__':
     store_num=4
     #parameter = {'选仓个数': store_num}
     percent_dir = r'../data/goods_layout_{}'.format(store_num-2)
     sale_dir1=r'../data/goods_layout_{}/toc_order_match.csv'.format(store_num)
     meizanchen = fee_calculation(percent_dir=percent_dir)
     storage_cost=meizanchen.cal_storage_cost()
     transport_cost_fdc=meizanchen.cal_transport_cost_fdc()
     #merged_replenish_grouped_repl, merged_replenish_df_xs_grouped, merged_replenish_df_dangerous, replenish_all_df_cdc
     a=meizanchen.cal_transport_cost_rdc()
     merged_replenish_grouped_repl = a[0]
     merged_replenish_df_xs_grouped = a[1]
     merged_replenish_df_dangerous = a[2]
     replenish_all_df_cdc = a[3]
     cal_delivery_toB_cost = meizanchen.cal_delivery_toB_cost()
     cal_delivery_toC_cost=meizanchen.cal_delivery_toC_cost(sale_dir1=sale_dir1)
     writer = pd.ExcelWriter('../data/goods_layout_{0}/方案{0}_费用计算结果.xlsx'.format(store_num))
     store_cost=pd.DataFrame(storage_cost)
     transport_cost_fdc = pd.DataFrame(transport_cost_fdc)
     common_replenish_cost = pd.DataFrame(merged_replenish_grouped_repl)
     xs_replenish_cost = pd.DataFrame(merged_replenish_df_xs_grouped)
     dangerous_replenish_cost = pd.DataFrame(merged_replenish_df_dangerous)
     cdc_replenish_cost = pd.DataFrame(replenish_all_df_cdc)
     cal_delivery_toB_cost = pd.DataFrame(cal_delivery_toB_cost)
     cal_delivery_toC_cost = pd.DataFrame(cal_delivery_toC_cost)
     dict={'仓储费用':[store_cost['仓储费用'].sum()],'fdc补货运输费用':[transport_cost_fdc['补货运输费用'].sum()],
           'fdc补货装卸费用':[transport_cost_fdc['装卸费用'].sum()],
           'toB补货运输费用':[cal_delivery_toB_cost['补货运输费用'].sum()],
           'toB补货装卸费用':[cal_delivery_toB_cost['装卸费用'].sum()],
           'toC配送费用':[cal_delivery_toC_cost['配送费'].sum()],
           'toC分拣费用':[cal_delivery_toC_cost['分拣费'].sum()],
           'rdc普通品补货运输费用':[common_replenish_cost['repl_cost_sum'].sum()],
           'rdcXS饮料补货运输费用':[xs_replenish_cost['repl_cost_sum'].sum()],
           'rdc危险品补货运输费用':[dangerous_replenish_cost['repl_cost_sum'].sum()],
           'cdc外购品补货运输费用':[cdc_replenish_cost['repl_cost_sum'].sum()],
           'dc外购品补货运输费用':[cdc_replenish_cost['repl_cost_sum_装卸费'].sum()],
           '总费用':[store_cost['仓储费用'].sum()+transport_cost_fdc['补货运输费用'].sum()+transport_cost_fdc['装卸费用'].sum()
                  +cal_delivery_toB_cost['补货运输费用'].sum()+cal_delivery_toB_cost['装卸费用'].sum()+cal_delivery_toC_cost['配送费'].sum()
                  +cal_delivery_toC_cost['分拣费'].sum()+common_replenish_cost['repl_cost_sum'].sum()+xs_replenish_cost['repl_cost_sum'].sum()
                  +dangerous_replenish_cost['repl_cost_sum'].sum()+cdc_replenish_cost['repl_cost_sum'].sum()+cdc_replenish_cost['repl_cost_sum_装卸费'].sum()]}
     sum=pd.DataFrame(list(dict.items()),columns=['项目','费用'])
     print(sum)
     print('总费用为：',sum['费用'].sum()[-1])
     store_cost.to_excel(writer, sheet_name='仓储费用')
     transport_cost_fdc.to_excel(writer, sheet_name='fdc补货运输分拣费用')
     cal_delivery_toB_cost.to_excel(writer, sheet_name='toB补货运输费用')
     cal_delivery_toC_cost.to_excel(writer, sheet_name='toC配送分拣费用')
     common_replenish_cost.to_excel(writer, sheet_name='普通品rdc补货费用')
     xs_replenish_cost.to_excel(writer, sheet_name='XS饮料补货费用')
     dangerous_replenish_cost.to_excel(writer, sheet_name='危险品工厂补货费用')
     cdc_replenish_cost.to_excel(writer, sheet_name='外购品rdc补货装卸费用')
     sum.to_excel(writer, sheet_name='汇总')
     writer.save()
     print('费用计算完成！！！')


