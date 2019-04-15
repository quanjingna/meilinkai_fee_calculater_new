# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import math
from os import listdir
from os.path import isfile, join
import sys

# 设置显示全部行列
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)

class fee_calculation(object):

    def __init__(self, sku_df, fee_dir, percent_dir=None):
        """
        初始化
        """
        self.percent_dir = percent_dir
        print('当前计算文件:{}'.format(self.percent_dir))
        # 当前日期
        self.today = ""
        self.stock_df = None
        self.sku_info_df = sku_df
        self.sku_info_df=self.sku_info_df[['sku_id','产地','属性','类型','支/板','商品毛重（公斤）']]
        print('sku_info_df 去重前{}'.format(self.sku_info_df.shape))
        self.sku_info_df=self.sku_info_df.drop_duplicates()
        print('sku_info_df 去重后{}'.format(self.sku_info_df.shape))
        # self.sku_info_df=self.sku_info_df.rename(columns={'商家商品编码':'sku_id'})
        self.sku_info_df = self.sku_info_df[['sku_id','产地','属性','类型','支/板','商品毛重（公斤）']]
        self.sku_info_df['sku_id']=self.sku_info_df['sku_id'].astype(str)
        print('sku 去重前{}'.format(self.sku_info_df.shape))
        self.sku_info_df['sku_id'] = self.sku_info_df['sku_id'].apply(lambda x: x.strip())
        self.sku_info_df = self.sku_info_df.drop_duplicates(subset=['sku_id'])
        print('sku 去重后{}'.format(self.sku_info_df.shape))
        print('商品信息表       sku_info_df read over!')
        self.factory_storage_fee_df  = pd.read_excel(fee_dir,sheet_name='工厂-总仓' )
        self.factory_storage_fee_df=self.factory_storage_fee_df.drop_duplicates()
        print('工厂-总仓费用    factory_storage_fee_df read over!')
        self.central_R_fee_df = pd.read_excel(fee_dir, sheet_name='总-R仓')
        self.central_R_fee_df = self.central_R_fee_df.drop_duplicates()
        print('总-R仓费用       central_R_fee_df read over!')
        self.factory_R_fee_df = pd.read_excel(fee_dir, sheet_name='工厂-R')
        self.factory_R_fee_df =self.factory_R_fee_df.drop_duplicates()
        print('工厂-R仓费用     factory_R_fee_df read over!')
        self.R_F_fee_df = pd.read_excel(fee_dir, sheet_name='R-F仓')
        self.R_F_fee_df=self.R_F_fee_df.drop_duplicates()
        print('R-F仓费用        R_F_fee_df read over!')
        self.storage_city_fee_df = pd.read_excel(fee_dir, sheet_name='仓—城市')
        self.storage_city_fee_df=self.storage_city_fee_df.drop_duplicates()
        print('仓-城市费用      storage_city_fee_df read over!')
        self.R_store_fee_df = pd.read_excel(fee_dir, sheet_name='R-门店')
        self.R_store_fee_df=self.R_store_fee_df.drop_duplicates()
        print('R-门店费用       R_store_fee_df read over!')

    def cal_storage_rdc_cost(self, percent_dir=None):
        """
        根据sku数量和仓库计算仓储费
        """
        # 得到所有库存文件
        print()
        print('============开始计算rdc库存成本 {} ==============='.format(self.percent_dir))
        stock_file_path = self.percent_dir+'/stock'
        print('当前计算文件夹：{}'.format(stock_file_path))
        stock_files = [f for f in listdir(stock_file_path) if   'RDC' in f  or  'CDC' in f and isfile(join(stock_file_path, f))]
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

        # 合并sku信息
        merged_stock_df = pd.merge(left=stock_all_df, right=self.sku_info_df, left_on='sku_id', right_on='sku_id', how='left')
        print('RDC仓SKU个数：',len(merged_stock_df['sku_id'].drop_duplicates()))

        #判断数据是否增加，排除sku重复信息
        if merged_stock_df.shape[0] > stock_all_df.shape[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        print('商品信息合并完成，总数据量{}', merged_stock_df.shape)  # 合并后数据量
        # 判断是否有sku或者仓库信息找不到
        null_sku_df = merged_stock_df.loc[merged_stock_df['sku_id'].isnull()| merged_stock_df['store_city'].isnull()]
        if null_sku_df.shape[0] > 0:
            print(null_sku_df)
            print('存在sku或仓找不到的情况')
            print('程序退出')
            sys.exit(-1)
        # 计算每个sku的板数
        #第一次合并
        merged_stock_df=merged_stock_df[['date','sku_id','类型','store_city','sku_qty', '支/板']].groupby(['date','sku_id','类型','store_city'],as_index=False).agg( {'sku_qty':'sum','支/板':'mean'})
        merged_stock_df['plate_num'] = merged_stock_df['sku_qty']/merged_stock_df['支/板']
        merged_stock_df['plate_num']=merged_stock_df['plate_num'].apply(lambda x: 0 if np.isinf(x) else x ) #为后续计算需转换数量为0的板数为0
        #第二次合并，汇总一天内同一地点同种类型商品的板数
        merged_stock_df_group = merged_stock_df[['date', 'store_city', '类型', 'plate_num']].groupby(
            ['date', 'store_city', '类型'],as_index=False).agg({'plate_num': 'sum'})

        # 从费用报价中整理出仓储费用
        tmp_RDC_fee_2018 = pd.melt(self.central_R_fee_df,
                               id_vars='RDC',
                               value_vars=list(self.central_R_fee_df.columns[8:14]),  # list of days of the week
                               var_name='RDC_class',
                               value_name='仓租')
        tmp_RDC_fee_2018['RDC_class'] = tmp_RDC_fee_2018['RDC_class'].map(lambda x: x.lstrip('RDC').rstrip('仓租2018'))
        tmp_RDC_fee_2018['RDC_class'] = tmp_RDC_fee_2018['RDC_class'].apply(lambda x: 'XS饮料' if x == 'XS' else x)
        tmp_RDC_fee_2018=tmp_RDC_fee_2018.rename(columns=({'仓租':'仓租2018'}))
        tmp_RDC_fee_2017 = pd.melt(self.central_R_fee_df,
                                   id_vars='RDC',
                                   value_vars=list(self.central_R_fee_df.columns[20:26]),  # list of days of the week
                                   var_name='RDC_class',
                                   value_name='仓租')
        tmp_RDC_fee_2017['RDC_class'] = tmp_RDC_fee_2017['RDC_class'].map(lambda x: x.lstrip('RDC').rstrip('仓租2017'))
        tmp_RDC_fee_2017['RDC_class'] = tmp_RDC_fee_2017['RDC_class'].apply(lambda x: 'XS饮料' if x == 'XS' else x)
        tmp_RDC_fee_2017 = tmp_RDC_fee_2017.rename(columns=({'仓租': '仓租2017'}))
        #合并两年的费用报价
        temp_RDC_fee_group=pd.merge(tmp_RDC_fee_2018,tmp_RDC_fee_2017,on=['RDC','RDC_class'],how='left')
        temp_RDC_fee_group=temp_RDC_fee_group.drop_duplicates()
        # print(temp_RDC_fee_group)
        temp = merged_stock_df_group.shape
        merged_stock_df_group = pd.merge(left=merged_stock_df_group, right=temp_RDC_fee_group[['RDC', 'RDC_class', '仓租2018','仓租2017']],
                                   left_on=['store_city', '类型'], right_on=['RDC', 'RDC_class'], how='left')
        #print(merged_stock_df_group.shape)
        if merged_stock_df_group.shape[0]> temp[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        # 根据sku类型计算该sku的报价
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
        # 板数取整，计算仓储费用
        merged_stock_df_group = merged_stock_df_group[
            ['store_city', '仓租2018', '仓租2017','plate_num']].groupby(['store_city'],
             as_index=False).agg(
            {'plate_num': 'sum', '仓租2018': 'mean', '仓租2017': 'mean'})
        merged_stock_df_group['plate_num_int'] = merged_stock_df_group['plate_num'].apply(lambda x: fun(x))
        merged_stock_df_group['stock_cost_sum_2018'] = merged_stock_df_group['plate_num_int']*merged_stock_df_group['仓租2018']
        merged_stock_df_group['stock_cost_sum_2017'] = merged_stock_df_group['plate_num_int'] * merged_stock_df_group['仓租2017']
        print('使用板数计算完成')
        # merged_stock_df_group.to_csv(r'../new1220/test/rdc_库存仓储_24_2.csv')
        #第三次合并，计算同一类型同一地点的仓储费用
        merged_stock_df_group= merged_stock_df_group[['store_city','stock_cost_sum_2018','stock_cost_sum_2017']].groupby(['store_city'],
         as_index=False).agg({'stock_cost_sum_2018': 'sum','stock_cost_sum_2017':'sum'})
        print('总价格计算完成')
        merged_stock_df_group=merged_stock_df_group.rename(columns={'store_city':'仓库城市','stock_cost_sum_2018':'2018仓储费用','stock_cost_sum_2017':'2017仓储费用'})
        print('==================')
        print('2018rdc库存费最终成本为:{}'.format(merged_stock_df_group['2018仓储费用'].sum()))
        print('2017rdc库存费最终成本为:{}'.format(merged_stock_df_group['2017仓储费用'].sum()))
        print('=================库存费 {} OVER ======================'.format(self.percent_dir))
        return  merged_stock_df_group

    def cal_storage_fdc_cost(self, percent_dir=None):
        """
        根据sku数量和仓库计算仓储费
        """
        # 得到所有库存文件
        print()
        print('============开始计算fdc库存成本 {} ==============='.format(self.percent_dir))
        stock_file_path = self.percent_dir + '/stock'
        print('当前计算文件夹：{}'.format(stock_file_path))
        stock_files = [f for f in listdir(stock_file_path) if 'FDC' in f and isfile(join(stock_file_path, f))]
        print('文件个数:{}'.format(len(stock_files)))
        if len(stock_files)!=0:
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
                print('{}:  {} \t\t{} ------>  {} '.format(i + 1, stock_file, shape0, tmp_stock_df.shape[0]))
                stock_df_list.append(tmp_stock_df)

            # 合并为一个文件
            stock_all_df = pd.concat(stock_df_list)
            print('合并完成，总数据量{}'.format(stock_all_df.shape))
            # 合并sku信息
            merged_stock_df = pd.merge(left=stock_all_df, right=self.sku_info_df, left_on='sku_id', right_on='sku_id',
                                       how='left')
            print('FDC仓SKU个数：', len(merged_stock_df['sku_id'].drop_duplicates()))
            # print(merged_stock_df.shape)  # 合并后数据量
            # 判断数据是否增加，排除sku重复信息
            if merged_stock_df.shape[0] > stock_all_df.shape[0]:
                print('存在sku数据增多的情况')
                print('程序退出')
                sys.exit(-1)
            print('merge sku info 完成')
            # 判断是否有sku或者仓库信息找不到
            null_sku_df = merged_stock_df.loc[
                merged_stock_df['sku_id'].isnull() | merged_stock_df['store_city'].isnull()]
            if null_sku_df.shape[0] > 0:
                print(null_sku_df)
                print('存在sku找不到的情况')
                print('程序退出')
                sys.exit(-1)
            # 计算每个sku的板数
            # 第一次合并
            merged_stock_df = merged_stock_df[['date', 'sku_id', '类型', 'store_city', 'sku_qty', '支/板']].groupby(
                ['date', 'sku_id', '类型', 'store_city'], as_index=False).agg(
                {'sku_qty': 'sum', '支/板': 'mean'})
            merged_stock_df['plate_num'] = merged_stock_df['sku_qty'] / merged_stock_df['支/板']
            merged_stock_df['plate_num'] = merged_stock_df['plate_num'].apply(
                lambda x: 0 if np.isinf(x) else x)  # 为后续计算需转换数量为0的板数为0
            # 第二次合并，汇总一天内同一地点同种类型商品的板数
            merged_stock_df_group = merged_stock_df[['date', 'store_city', '类型', 'plate_num']].groupby(
                ['date', 'store_city', '类型'], as_index=False).agg({'plate_num': 'sum'})

            # 从费用报价中整理出仓储费用

            tmp_FDC_fee_2018 = pd.melt(self.R_F_fee_df,
                                       id_vars='FDC',
                                       value_vars=list(self.R_F_fee_df.columns[7:13]),  # list of days of the week
                                       var_name='FDC_class',
                                       value_name='仓租')
            tmp_FDC_fee_2018['FDC_class'] = tmp_FDC_fee_2018['FDC_class'].map(
                lambda x: x.lstrip('FDC').rstrip('仓租2018'))
            tmp_FDC_fee_2018['FDC_class'] = tmp_FDC_fee_2018['FDC_class'].apply(lambda x: 'XS饮料' if x == 'XS' else x)
            tmp_FDC_fee_2018 = tmp_FDC_fee_2018.rename(columns=({'仓租': '仓租2018'}))
            tmp_FDC_fee_2017 = pd.melt(self.R_F_fee_df,
                                       id_vars='FDC',
                                       value_vars=list(self.R_F_fee_df.columns[18:24]),  # list of days of the week
                                       var_name='FDC_class',
                                       value_name='仓租')
            tmp_FDC_fee_2017['FDC_class'] = tmp_FDC_fee_2017['FDC_class'].map(
                lambda x: x.lstrip('FDC').rstrip('仓租2017'))
            tmp_FDC_fee_2017['FDC_class'] = tmp_FDC_fee_2017['FDC_class'].apply(lambda x: 'XS饮料' if x == 'XS' else x)
            tmp_FDC_fee_2017 = tmp_FDC_fee_2017.rename(columns=({'仓租': '仓租2017'}))
            # 合并两年的费用报价
            temp_FDC_fee_group = pd.merge(tmp_FDC_fee_2018, tmp_FDC_fee_2017, on=['FDC', 'FDC_class'], how='left')
            temp_FDC_fee_group = temp_FDC_fee_group.drop_duplicates()
            # print(temp_FDC_fee_group)
            temp = merged_stock_df_group.shape
            merged_stock_df_group = pd.merge(left=merged_stock_df_group,
                                             right=temp_FDC_fee_group[['FDC', 'FDC_class', '仓租2018', '仓租2017']],
                                             left_on=['store_city', '类型'], right_on=['FDC', 'FDC_class'], how='left')
            # print(merged_stock_df_group.shape)
            if merged_stock_df_group.shape[0] > temp[0]:
                print('存在sku数据增多的情况')
                print('程序退出')
                sys.exit(-1)

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

            # 板数取整，计算仓储费用
            merged_stock_df_group = merged_stock_df_group[
                ['store_city', '仓租2018', '仓租2017', 'plate_num']].groupby(['store_city'],
                                                                         as_index=False).agg(
                {'plate_num': 'sum', '仓租2018': 'mean', '仓租2017': 'mean'})
            merged_stock_df_group['plate_num_int'] = merged_stock_df_group['plate_num'].apply(lambda x: fun(x))
            # 根据sku类型计算该sku的报价
            merged_stock_df_group['stock_cost_sum_2018'] = merged_stock_df_group['plate_num_int'] * \
                                                           merged_stock_df_group[
                                                               '仓租2018']
            merged_stock_df_group['stock_cost_sum_2017'] = merged_stock_df_group['plate_num_int'] * \
                                                           merged_stock_df_group[
                                                               '仓租2017']
            print('使用板数计算完成')
            # merged_stock_df_group.to_csv(r'../new1220/test/stock_库存仓储.csv')
            # 第三次合并，计算同一类型同一地点的仓储费用
            merged_stock_df_group = merged_stock_df_group[
                ['store_city', 'stock_cost_sum_2018', 'stock_cost_sum_2017']].groupby(['store_city'],
                                                                                      as_index=False).agg(
                {'stock_cost_sum_2018': 'sum', 'stock_cost_sum_2017': 'sum'})
            print('总价格计算完成')
            merged_stock_df_group = merged_stock_df_group.rename(
                columns={'store_city': '仓库城市', 'stock_cost_sum_2018': '2018仓储费用', 'stock_cost_sum_2017': '2017仓储费用'})
            print('==================')
            print('2018fdc库存费最终成本为:{}'.format(merged_stock_df_group['2018仓储费用'].sum()))
            print('2017fdc库存费最终成本为:{}'.format(merged_stock_df_group['2017仓储费用'].sum()))
            print('=================库存费 {} OVER ======================'.format(self.percent_dir))
            return merged_stock_df_group
        else:
            return pd.DataFrame()

    def cal_transport_cost_fdc(self, percent_dir=None):
        """
        根据sku数量，出发仓库和目的仓库计算运费
        """
        # 得到所有补货文件
        print()
        print('============开始计算FDC补货费（运输费和装卸费） {}==========='.format(self.percent_dir))
        replenish_file_path = self.percent_dir+'/repleshment'
        print('当前计算文件夹：{}'.format(replenish_file_path))
        replenish_files_fdc = [f for f in listdir(replenish_file_path) if  'FDC' in f and isfile(join(replenish_file_path, f))]
        print('文件个数====== {}'.format(len(replenish_files_fdc)))
        if len(replenish_files_fdc)!=0:
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
            temp1 = replenish_all_df.shape
            merged_replenish_df = pd.merge(left=replenish_all_df, right=self.sku_info_df, left_on='sku_id',
                                           right_on='sku_id', how='left')
            # print(self.percent_dir)
            fdc_df = pd.read_csv(open(self.percent_dir + '/RDC_FDC.csv', encoding='utf-8'))
            fdc_df = fdc_df.drop_duplicates()
            if replenish_all_df.shape[0] > temp1[0]:
                print('存在sku数据增多的情况')
                print('程序退出')
                sys.exit(-1)
            # print(fdc_df )
            temp2 = merged_replenish_df.shape
            merged_replenish_df = pd.merge(merged_replenish_df, fdc_df[['FDC', 'RDC']], left_on='store_city',
                                           right_on='FDC', how='left')
            if merged_replenish_df.shape[0] > temp2[0]:
                print('存在sku数据增多的情况')
                print('程序退出')
                sys.exit(-1)
            # print(merged_replenish_df)
            # 拆分危险品与其他商品
            print('fdc补货总数据量为：', merged_replenish_df.shape)

            ###################################################计算普通品情况
            # 合并同一天的体积重量
            merged_replenish_df_grouped = merged_replenish_df[
                ['date', 'sku_id', '类型', 'RDC', 'store_city', 'sku_qty', '支/板']].groupby(
                ['date', 'sku_id', '类型',  'RDC', 'store_city'], as_index=False).agg(
                {'sku_qty': 'sum', '支/板': 'mean'})
            # 添加配送费用报价信息
            temp3 = merged_replenish_df_grouped.shape
            merged_replenish_df_grouped = pd.merge(left=merged_replenish_df_grouped, right=self.R_F_fee_df[
                ['RDC', 'FDC', '普通品运输费2018', '普通品装卸费2018', '普通品运输费2017', '普通品装卸费2017']],
                                                   left_on=['RDC', 'store_city'], right_on=['RDC', 'FDC'], how='left')
            print(merged_replenish_df_grouped.shape)
            if merged_replenish_df_grouped.shape[0] > temp3[0]:
                print('存在sku数据增多的情况')
                print('程序退出')
                sys.exit(-1)

            merged_replenish_df_grouped['plate_num'] = merged_replenish_df_grouped['sku_qty'] / \
                                                       merged_replenish_df_grouped['支/板']
            merged_replenish_df_grouped['plate_num'] = merged_replenish_df_grouped['plate_num'].apply(
                lambda x: 0 if np.isinf(x) else x)
            # 第一次合并
            merged_replenish_df_grouped = merged_replenish_df_grouped[
                ['RDC', 'store_city', 'plate_num', '普通品运输费2018', '普通品装卸费2018', '普通品运输费2017',
                 '普通品装卸费2017']].groupby(
                ['RDC', 'store_city'], as_index=False).agg(
                {'普通品装卸费2018': 'mean', '普通品运输费2018': 'mean', '普通品装卸费2017': 'mean', '普通品运输费2017': 'mean',
                 'plate_num': 'sum'})

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

            merged_replenish_df_grouped['plate_num_int'] = merged_replenish_df_grouped['plate_num'].apply(
                lambda x: fun(x))
            merged_replenish_df_grouped['repl_cost_sum_2018'] = merged_replenish_df_grouped['plate_num_int'] * \
                                                                merged_replenish_df_grouped['普通品运输费2018']
            merged_replenish_df_grouped['repl_cost_sum_2018装卸费'] = merged_replenish_df_grouped['plate_num_int'] * \
                                                                   merged_replenish_df_grouped['普通品装卸费2018']
            merged_replenish_df_grouped['repl_cost_sum_2017'] = merged_replenish_df_grouped['plate_num_int'] * \
                                                                merged_replenish_df_grouped['普通品运输费2017']
            merged_replenish_df_grouped['repl_cost_sum_2017装卸费'] = merged_replenish_df_grouped['plate_num_int'] * \
                                                                   merged_replenish_df_grouped['普通品装卸费2017']

            # 第二次合并
            merged_replenish_df_grouped = merged_replenish_df_grouped[
                ['RDC', 'store_city',  'repl_cost_sum_2018', 'repl_cost_sum_2018装卸费', 'repl_cost_sum_2017',
                 'repl_cost_sum_2017装卸费']]. \
                groupby(['RDC', 'store_city', ], as_index=False).agg(
                {'repl_cost_sum_2018': 'sum', 'repl_cost_sum_2018装卸费': 'sum', 'repl_cost_sum_2017': 'sum',
                 'repl_cost_sum_2017装卸费': 'sum'})
            # print(merged_replenish_grouped_repl)
            print('fdc普通品补货总价格计算完成')
            merged_replenish_grouped_repl_comb = merged_replenish_df_grouped.rename(
                columns={'store_city': '仓库城市', 'repl_cost_sum_2018': '2018补货运输费用', 'repl_cost_sum_2018装卸费': '2018装卸费用',
                         'repl_cost_sum_2017': '2017补货运输费用', 'repl_cost_sum_2017装卸费': '2017装卸费用'})
            # print(merged_replenish_grouped_repl_comb)
            print('2018补货运费最终为:{}'.format(merged_replenish_grouped_repl_comb['2018补货运输费用'].sum()))
            print('2018装卸费用最终为:{}'.format(merged_replenish_grouped_repl_comb['2018装卸费用'].sum()))
            print('2017补货运费最终为:{}'.format(merged_replenish_grouped_repl_comb['2017补货运输费用'].sum()))
            print('2017装卸费用最终为:{}'.format(merged_replenish_grouped_repl_comb['2017装卸费用'].sum()))
            print('=================补货运输费/装卸货运费 {} OVER ======================'.format(self.percent_dir))
            return (merged_replenish_grouped_repl_comb)
        else:
            return pd.DataFrame()

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
        replenish_files_fdc = [f for f in listdir(replenish_file_path) if  'RDC' in f or 'CDC' in f and isfile(join(replenish_file_path, f))]
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
        replenish_all_df['sku_id']=replenish_all_df['sku_id'].astype('str')
        # merge sku info
        temp7=replenish_all_df.shape
        merged_replenish_df = pd.merge(left=replenish_all_df, right=self.sku_info_df[['sku_id','属性','产地','类型','支/板']], left_on='sku_id', right_on='sku_id', how='left')
        if merged_replenish_df.shape[0]> temp7[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        # 按照商品属性分开数据
        print('原始数据量：', merged_replenish_df.shape)
        #增加总仓
        merged_replenish_df['总仓'] = '杭州'
        ###此处为广州工厂
        #计算短驳运输费用（xs由咸宁运至武汉总仓，面膜由中山运至广州，危险品由启动运至广州）
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
        ##############################################计算运输费用
        tmp_central_RDC_fee_2018 = pd.melt(self.central_R_fee_df,
                                   id_vars=['总仓','RDC'],
                                   value_vars=list(self.central_R_fee_df.columns[3:6]),  # list of days of the week
                                   var_name='RDC_class',
                                   value_name='运输费')
        tmp_central_RDC_fee_2018 = tmp_central_RDC_fee_2018.loc[tmp_central_RDC_fee_2018['运输费'].notnull()]
        tmp_central_RDC_fee_2018['RDC_class'] = tmp_central_RDC_fee_2018['RDC_class'].map(lambda x: x.rstrip('运输费2018'))
        tmp_central_RDC_fee_2018 = tmp_central_RDC_fee_2018.rename(columns=({'运输费': '运输费2018'}))
        tmp_central_RDC_fee_2017 = pd.melt(self.central_R_fee_df,
                                   id_vars=['总仓','RDC'],
                                   value_vars=list(self.central_R_fee_df.columns[15:18]),  # list of days of the week
                                   var_name='RDC_class',
                                   value_name='运输费')
        tmp_central_RDC_fee_2017 = tmp_central_RDC_fee_2017.loc[tmp_central_RDC_fee_2017['运输费'].notnull()]
        tmp_central_RDC_fee_2017['RDC_class'] = tmp_central_RDC_fee_2017['RDC_class'].map(lambda x: x.rstrip('运输费2017'))
        tmp_central_RDC_fee_2017 = tmp_central_RDC_fee_2017.rename(columns=({'运输费': '运输费2017'}))
        # 合并两年的费用报价
        tmp_central_RDC_fee_group = pd.merge(tmp_central_RDC_fee_2018, tmp_central_RDC_fee_2017, on=['总仓','RDC', 'RDC_class'], how='left')
        tmp_central_RDC_fee_group = tmp_central_RDC_fee_group.drop_duplicates()
        #print(tmp_central_RDC_fee_group)
        # 增加装卸费成本
        tmp_central_RDC_load_fee_2018 = pd.melt(self.central_R_fee_df,
                                           id_vars=['总仓', 'RDC'],
                                           value_vars=list(self.central_R_fee_df.columns[6:8]),
                                           # list of days of the week
                                           var_name='RDC_class',
                                           value_name='装卸费')
        # tmp_central_RDC_load_fee_2018 = tmp_central_RDC_load_fee_2018.loc[tmp_central_RDC_load_fee_2018['装卸费'].notnull()]
        tmp_central_RDC_load_fee_2018['RDC_class'] = tmp_central_RDC_load_fee_2018['RDC_class'].map(lambda x: x.rstrip('装卸费2018'))
        tmp_central_RDC_load_fee_2018 = tmp_central_RDC_load_fee_2018.rename(columns=({'装卸费': '装卸费2018'}))
        tmp_central_RDC_load_fee_2017 = pd.melt(self.central_R_fee_df,
                                           id_vars=['总仓', 'RDC'],
                                           value_vars=list(self.central_R_fee_df.columns[18:20]),
                                           # list of days of the week
                                           var_name='RDC_class',
                                           value_name='装卸费')
        # tmp_central_RDC_load_fee_2017 = tmp_central_RDC_load_fee_2017.loc[tmp_central_RDC_load_fee_2017['装卸费'].notnull()]
        tmp_central_RDC_load_fee_2017['RDC_class'] = tmp_central_RDC_load_fee_2017['RDC_class'].map(lambda x: x.rstrip('装卸费2017'))
        tmp_central_RDC_load_fee_2017 = tmp_central_RDC_load_fee_2017.rename(columns=({'装卸费': '装卸费2017'}))
        tmp_central_RDC_load_fee_group = pd.merge(tmp_central_RDC_load_fee_2018, tmp_central_RDC_load_fee_2017,
                                             on=['总仓', 'RDC', 'RDC_class'], how='left')
        tmp_central_RDC_load_fee_group = tmp_central_RDC_load_fee_group.drop_duplicates()
        # print(tmp_central_RDC_load_fee_group)
        ############################################开始计算总仓到各RDC运输及装卸费
        concat_group=merged_replenish_df
        concat_group['plate_num'] = concat_group['sku_qty'] /  concat_group['支/板']
        concat_group['plate_num'] = concat_group['plate_num'].apply( lambda x: 0 if np.isinf(x) else x)
        # print(concat_group.columns)
        concat_group = concat_group.groupby(
            [ 'sku_id', 'store_city', '产地', '类型', '总仓'],
            as_index=False).agg(
            {'plate_num': 'sum'})
        temp9=concat_group.shape
        concat_group_fee=pd.merge(concat_group,tmp_central_RDC_fee_group,right_on=['总仓','RDC'],left_on=['总仓','store_city'],how='left')
        if concat_group_fee.shape[0] > temp9[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        temp10 = concat_group_fee.shape
        concat_group_fee = pd.merge(concat_group_fee, tmp_central_RDC_load_fee_group, right_on=['总仓', 'RDC'],
                                    left_on=['总仓', 'store_city'], how='left')
        if concat_group_fee.shape[0] > temp10[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        # print(concat_group_fee)
        # print(concat_group_fee.columns)
        concat_group_fee_group = concat_group_fee.groupby(
            [ '总仓', 'store_city', '产地', '类型'],
            as_index=False).agg({'plate_num': 'sum','运输费2018': 'mean','运输费2017': 'mean','装卸费2018': 'mean','装卸费2017': 'mean'})
        concat_group_fee_group['plate_num_int'] = concat_group_fee_group['plate_num'].apply(lambda x: fun(x))
        concat_group_fee_group['repl_cost_sum_2018'] = concat_group_fee_group['plate_num_int'] * \
                                                       concat_group_fee_group['运输费2018']
        concat_group_fee_group['repl_cost_sum_2017'] = concat_group_fee_group['plate_num_int'] * \
                                                       concat_group_fee_group['运输费2017']
        concat_group_fee_group['repl_cost_装卸费_2018'] = concat_group_fee_group['plate_num_int'] * \
                                                       concat_group_fee_group['装卸费2018']
        concat_group_fee_group['repl_cost_装卸费_2017'] = concat_group_fee_group['plate_num_int'] * \
                                                       concat_group_fee_group['装卸费2017']
        concat_group_fee_group_repl = concat_group_fee_group[
            ['产地', '类型','总仓', 'repl_cost_sum_2018', 'repl_cost_sum_2017', 'repl_cost_装卸费_2018',
             'repl_cost_装卸费_2017']].groupby(['产地', '类型','总仓'], as_index=False).agg(
            {'repl_cost_sum_2018': 'sum', 'repl_cost_sum_2017': 'sum', 'repl_cost_装卸费_2018': 'sum',
             'repl_cost_装卸费_2017': 'sum'})

        concat_group_fee_group_repl.index = pd.Series(['总仓-rdc'])
        #print(concat_group_fee_group_repl)
        print('rdc工厂补货装卸2018总价格:',concat_group_fee_group_repl['repl_cost_sum_2018'].sum()+concat_group_fee_group_repl['repl_cost_装卸费_2018'].sum())
        print('rdc工厂补货装卸2017总价格:',concat_group_fee_group_repl['repl_cost_sum_2017'].sum()+concat_group_fee_group_repl['repl_cost_装卸费_2017'].sum())
        print('rdc补货分拣总价格计算完成')
        comb_file=concat_group_fee_group_repl
        comb_file = comb_file.rename(
            columns={'repl_cost_装卸费_2018': '2018装卸费', 'repl_cost_装卸费_2017': '2017装卸费','repl_cost_sum_2018': '2018运输费', 'repl_cost_sum_2017': '2017运输费'})
        print(comb_file)
        print('=================进仓费/补货运费 {} OVER ======================'.format(self.percent_dir))
        return (comb_file)

    def cal_delivery_toC_cost(self,sale_dir=None):
        toc_data = pd.read_csv(open(sale_dir, encoding='utf-8'),
                               usecols=['id', 'date', 'sku_id', 'qty', 'store_city', 'end_city'])
        # toc_data = pd.read_csv(open(sale_dir,encoding='utf-8'), usecols=['id', 'date', 'sku_id', 'qty', 'start_city', 'end_city'])
        print('sale_toc read over!')
        print('订单数据量为',len(toc_data['id'].drop_duplicates()))
        toc_data['sku_id'] = toc_data['sku_id'].apply(lambda x: str(x).strip())
        # merge sku info
        temp17=toc_data.shape
        merged_toc_df = pd.merge(left=toc_data, right=self.sku_info_df[['sku_id', '类型', '商品毛重（公斤）']], left_on='sku_id',
                                 right_on='sku_id',
                                 how='left')
        if merged_toc_df.shape[0] > temp17[0]:
            print('存在sku数据增多的情况')
            print('程序退出')
            sys.exit(-1)
        print('sku_info_df 合并完成')
        print('配送数据量为：', merged_toc_df.shape)

        # 合并同一订单的体积重量
        merged_toc_df['weight_sum']=merged_toc_df['商品毛重（公斤）']*merged_toc_df['qty']
        merged_toc_df_grouped = merged_toc_df.groupby(
            ['date', 'id',  'store_city', 'end_city'], as_index=False).agg(
            {'weight_sum': 'sum'})
        # merged_toc_df_grouped = merged_toc_df.groupby(
        #     ['date', 'id',  'start_city', 'end_city'], as_index=False).agg(
        #     {'weight_sum': 'sum'})
        #print(len(merged_toc_df_grouped))
        merged_toc_df_grouped = pd.merge(left=merged_toc_df_grouped,
                                            right=self.storage_city_fee_df[['startcityname','endcityname','配送首重2018', '续重报价2018','配送首重2017', '续重报价2017','2018分拣费','2017分拣费']],
                                            left_on=['store_city', 'end_city'],
                                            right_on=['startcityname','endcityname'], how='left')
        # print(merged_toc_df_grouped.columns)
        # 当匹配缺少时用0填充
        # 判断是否有sku或者仓库信息找不到
        null_sku_df = merged_toc_df_grouped.loc[
            merged_toc_df_grouped['配送首重2018'].isnull() | merged_toc_df_grouped['续重报价2018'].isnull()]
        if null_sku_df.shape[0] > 0:
            # print('存在sku或者仓库找不到的情况')
            print('存在报价数据缺少的情况')
            print('数据缺失量为',null_sku_df.shape)
            # sys.exit(-1)
        merged_toc_df_grouped['配送首重2018']=merged_toc_df_grouped['配送首重2018'].fillna(8)
        merged_toc_df_grouped['续重报价2018'] = merged_toc_df_grouped['续重报价2018'].fillna(0.7)
        merged_toc_df_grouped['配送首重2017'] = merged_toc_df_grouped['配送首重2017'].fillna(8)
        merged_toc_df_grouped['续重报价2017'] = merged_toc_df_grouped['续重报价2017'].fillna(0.7)
        merged_toc_df_grouped['2018分拣费'] = merged_toc_df_grouped['2018分拣费'].fillna(11.28)
        merged_toc_df_grouped['2017分拣费'] = merged_toc_df_grouped['2017分拣费'].fillna(14.31)
        print('数据填充完毕！')
        def class_fee(a, b, c):
            if a< 1:
                return b
            else:
                if math.modf((a-1)*2)[0]> 0.5:
                    return (math.modf((a-1)*2)[1] + 1)*c +b*1
                elif math.modf((a-1)*2)[0]==0:
                    return (math.modf((a - 1) * 2)[1]) * c + b * 1
                elif math.modf((a-1)*2)[0]==0.5:
                    return ((a - 1) * 2) * c + b * 1
                else:
                    return (math.modf((a - 1) * 2)[1]+0.5)*c + b*1

        merged_toc_df_grouped['2018配送费']=merged_toc_df_grouped.apply(lambda x: class_fee(x['weight_sum'],x['配送首重2018'],x['续重报价2018']),axis=1)
        merged_toc_df_grouped['2017配送费']=merged_toc_df_grouped.apply(lambda x: class_fee(x['weight_sum'],x['配送首重2017'],x['续重报价2017']),axis=1)

        # merged_toc_df_grouped['分拣费']=14.31
        merged_toc_df_grouped=merged_toc_df_grouped[['store_city','2018配送费','2017配送费','2018分拣费','2017分拣费']].groupby('store_city').agg(sum)
        print(merged_toc_df_grouped)
        print('2018分拣费最终为:{}'.format(merged_toc_df_grouped['2018分拣费'].sum()))
        print('2018配送费最终为:{}'.format(merged_toc_df_grouped['2018配送费'].sum()))
        print('2017分拣费最终为:{}'.format(merged_toc_df_grouped['2017分拣费'].sum()))
        print('2017配送费最终为:{}'.format(merged_toc_df_grouped['2017配送费'].sum()))
        print('=================分拣费/配送费 {} OVER ======================'.format(self.percent_dir))
        return (merged_toc_df_grouped)


def fee_main_flow(plan_name, area_name):

    # plan_name = '1、东北：杭州-沈阳'
    # parameter = {'选仓个数': store_num}
    sku_dir = pd.read_excel(r'../data/{}/sku信息：{}.xlsx'.format(plan_name, area_name))
    fee_dir = r'../data/{}/仓报价1：CDC-RDC .xlsx'.format(plan_name)
    percent_dir = r'../data/goods_layout_{}'.format(plan_name)
    sale_dir = r'../data/goods_layout_{}/order_recover.csv'.format(plan_name)
    anli = fee_calculation(sku_dir, fee_dir, percent_dir=percent_dir)
    storage_cost_rdc = anli.cal_storage_rdc_cost()
    storage_cost_fdc = anli.cal_storage_fdc_cost()
    transport_cost_fdc = anli.cal_transport_cost_fdc()
    comb_file_rdc = anli.cal_transport_cost_rdc()

    cal_delivery_toC_cost = anli.cal_delivery_toC_cost(sale_dir=sale_dir)
    file_name = r'../data/goods_layout_{0}/方案{0}_费用计算结果.xlsx'.format(plan_name)
    writer = pd.ExcelWriter(file_name)
    # store_cost_rdc = pd.DataFrame(storage_cost_rdc)
    # store_cost_fdc = pd.DataFrame(storage_cost_fdc)
    transport_cost_fdc = pd.DataFrame(transport_cost_fdc)
    comb_fee_rdc = pd.DataFrame(comb_file_rdc)

    cal_delivery_toC_cost = pd.DataFrame(cal_delivery_toC_cost)

    storage_cost_rdc_fee = 0 if storage_cost_rdc.shape[0]==0 else storage_cost_rdc['2018仓储费用'].sum()
    storage_cost_fdc_fee = 0 if storage_cost_fdc.shape[0]==0  else storage_cost_fdc['2018仓储费用'].sum()
    transport_cost_fdc_fee1 = 0 if transport_cost_fdc.shape[0] == 0 else transport_cost_fdc['2018补货运输费用'].sum()
    transport_cost_fdc_fee2 = 0 if transport_cost_fdc.shape[0] == 0 else transport_cost_fdc['2018装卸费用'].sum()
    cal_delivery_toC_cost_fee1 = 0 if cal_delivery_toC_cost.shape[0] == 0 else cal_delivery_toC_cost['2018配送费'].sum()
    cal_delivery_toC_cost_fee2 = 0 if cal_delivery_toC_cost.shape[0] == 0 else cal_delivery_toC_cost['2018分拣费'].sum()
    comb_fee_rdc_fee1 = 0 if comb_fee_rdc.shape[0] == 0 else comb_fee_rdc['2018装卸费'].sum()
    comb_fee_rdc_fee2 = 0 if comb_fee_rdc.shape[0] == 0 else comb_fee_rdc['2018运输费'].sum()

    all_fee = storage_cost_rdc_fee+storage_cost_fdc_fee+transport_cost_fdc_fee1+transport_cost_fdc_fee2+cal_delivery_toC_cost_fee1+cal_delivery_toC_cost_fee2\
              +comb_fee_rdc_fee1+comb_fee_rdc_fee2


    dict = {'2018费用': [storage_cost_rdc_fee,
                       storage_cost_fdc_fee,
                      transport_cost_fdc_fee1,
                       transport_cost_fdc_fee2,
                       cal_delivery_toC_cost_fee1,
                       cal_delivery_toC_cost_fee2,
                       comb_fee_rdc_fee1,
                       comb_fee_rdc_fee2,
                       all_fee]
            }

    sum = pd.DataFrame(dict, index=['rdc仓储费用', 'fdc仓储费用', 'fdc补货运输费用', 'fdc补货装卸费用', 'toC配送费用', 'toC分拣费用',
                                    'rdc补货运输费用', 'rdc装卸费用', '总费用'])
    print(sum)
    storage_cost_rdc.to_excel(writer, sheet_name='rdc仓储费用')
    storage_cost_fdc.to_excel(writer, sheet_name='fdc仓储费用')
    transport_cost_fdc.to_excel(writer, sheet_name='fdc补货运输分拣费用')
    comb_fee_rdc.to_excel(writer, sheet_name='rdc补货分拣费用')
    cal_delivery_toC_cost.to_excel(writer, sheet_name='toC配送分拣费用')
    sum.to_excel(writer, sheet_name='汇总')
    writer.save()
    print('费用计算完成！！！')


if __name__ == '__main__':
     store_num=24
     flg=2
     plan_name = '1、东北：杭州-沈阳'
     #parameter = {'选仓个数': store_num}
     sku_dir=pd.read_excel(r'E:\meilinkai\玫琳凯11套_测算数据整理_0413\{}\sku信息：东北.xlsx'.format(plan_name))
     fee_dir=r'E:\meilinkai\玫琳凯11套_测算数据整理_0413\{}\仓报价1：CDC-RDC .xlsx'.format(plan_name)
     percent_dir = r'E:\meilinkai\data\goods_layout_{0}'.format(plan_name)
     sale_dir=r'E:\meilinkai\data\goods_layout_{0}\order_recover.csv'.format(plan_name)
     anli = fee_calculation(percent_dir=percent_dir)
     storage_cost_rdc=anli.cal_storage_rdc_cost()
     storage_cost_fdc = anli.cal_storage_fdc_cost()
     transport_cost_fdc=anli.cal_transport_cost_fdc()
     comb_file_rdc=anli.cal_transport_cost_rdc()

     cal_delivery_toC_cost=anli.cal_delivery_toC_cost(sale_dir=sale_dir)
     file_name=r'E:\meilinkai\data\goods_layout_{0}\方案{0}_费用计算结果.xlsx'.format(plan_name)
     writer = pd.ExcelWriter(file_name)
     store_cost_rdc=pd.DataFrame(storage_cost_rdc)
     store_cost_fdc = pd.DataFrame(storage_cost_fdc)
     transport_cost_fdc = pd.DataFrame(transport_cost_fdc)
     comb_fee_rdc=pd.DataFrame(comb_file_rdc)

     cal_delivery_toC_cost = pd.DataFrame(cal_delivery_toC_cost)

     dict={'2018费用':[storage_cost_rdc['2018仓储费用'].sum(),
                     storage_cost_fdc['2018仓储费用'].sum(),
                     transport_cost_fdc['2018补货运输费用'].sum(),
                     transport_cost_fdc['2018装卸费用'].sum(),
                     cal_delivery_toC_cost['2018配送费'].sum(),
                     cal_delivery_toC_cost['2018分拣费'].sum(),
                     comb_fee_rdc['2018运输费'].sum(),
                     comb_fee_rdc['2018装卸费'].sum(),
                     storage_cost_rdc['2018仓储费用'].sum()
                     + storage_cost_fdc['2018仓储费用'].sum()
                     + transport_cost_fdc['2018补货运输费用'].sum()
                     + transport_cost_fdc['2018装卸费用'].sum()
                      + cal_delivery_toC_cost['2018配送费'].sum()
                     + cal_delivery_toC_cost['2018分拣费'].sum() + comb_fee_rdc['2018运输费'].sum() + comb_fee_rdc['2018装卸费'].sum()]
           }

     sum=pd.DataFrame(dict,index=['rdc仓储费用','fdc仓储费用','fdc补货运输费用','fdc补货装卸费用','toB补货运输费用','toC配送费用','toC分拣费用','rdc补货运输费用','rdc装卸费用','总费用'])
     print(sum)
     storage_cost_rdc.to_excel(writer, sheet_name='rdc仓储费用')
     storage_cost_fdc.to_excel(writer, sheet_name='fdc仓储费用')
     transport_cost_fdc.to_excel(writer, sheet_name='fdc补货运输分拣费用')
     comb_fee_rdc.to_excel(writer, sheet_name='rdc补货分拣费用')
     cal_delivery_toC_cost.to_excel(writer, sheet_name='toC配送分拣费用')
     sum.to_excel(writer, sheet_name='汇总')
     writer.save()
     print('费用计算完成！！！')


