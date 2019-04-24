# -*- coding: utf-8 -*-
"""
This class is for reading and getting data from sale_data.csv
example：
1. initializing xxx = SaleData(file_path)  # file_path is the path of sale_data.csv
2. get sales quantity of fixed cities(list) and sku(string). You can give start_date and end_date(optional)
    eg: xxx.get_sales(cities=["city1", "city2"], sku="123456")
"""
import math
import os
from datetime import datetime
import pandas as pd
import inventoryCal
import sku_info_1
import duration
import numpy as np


class SaleData:
    # initializing and read csv, default encoding is utf-8
    def __init__(self, cover_path, file_path, plan_name, sku_list=None, storeORshop='store_city', encoding="utf-8"):

        self.cover_df = pd.read_csv(cover_path, encoding='gbk', engine='python', sep='\t')

        self.cover_df = self.cover_df.rename(
            columns={self.cover_df.columns[0]: 'start_city', '覆盖省': 'end_province', '覆盖城市': 'end_city'})

        # self.cover_df  = pd.read_excel(cover_path, sheet_name='仓toC覆盖城市',dtype={'vlt(hour)':np.float}).dropna()
        # self.cover_df = self.cover_df.sort_values('vlt(hour)').drop_duplicates('end_city')

        if file_path.endswith(".xlsx") or file_path.endswith(".xls"):

            print(file_path)
            self.data = pd.read_excel(file_path, sheet_name='仓库ToC数据', dtype=
            {'sku_id': str, 'qty': np.float, 'date': str, storeORshop: str}).dropna()
            self.data = self.data[['id', 'sku_id', 'date', 'qty', storeORshop, 'dis_city']]
        else:
            self.data = pd.read_csv(file_path, encoding=encoding, usecols=['sku_id', 'date', 'qty', storeORshop], dtype=
            {'sku_id': str, 'qty': float, 'date': str, storeORshop: str}, low_memory=False).dropna()
        print("订单数据读取完成")
        self.reShapeSales()
        print("订单数据和覆盖关系表join完成")

        def func(origin, new):
            if origin == new:
                return origin
            else:
                return new

        self.data['new_start_city'] = self.data.apply(lambda x: func(x[storeORshop], x['start_city']), axis=1)
        print("订单数据匹配覆盖关系完成")
        self.data = self.data.drop([storeORshop, 'start_city'], axis=1)
        self.data = self.data.rename(columns={'new_start_city': storeORshop})
        self.data['date'] = self.data['date'].apply(lambda x: x.split(' ')[0])
        self.data['sku_id'] = self.data['sku_id'].apply(lambda x: x.strip())
        self.data.to_csv('../data/goods_layout_{}/order_recover.csv'.format(plan_name), index=False)
        self.data = self.data.groupby([storeORshop, 'sku_id', 'date'], as_index=False).agg(sum)
        if sku_list is not None:
            self.data = self.data[self.data["sku_id"].isin(sku_list)]
            self.sku_list = sku_list
        else:
            self.sku_list = list(self.data["sku_id"].drop_duplicates().values)

    # 根据覆盖关对原始订单处理的函数
    def reShapeSales(self):
        print("开始匹配覆盖关系")
        print("data shape{}".format(self.data.shape))
        self.data = self.data.merge(self.cover_df[['start_city', 'end_city']], how='outer', left_on='dis_city',
                                    right_on='end_city').dropna()
        print("data recover shape{}".format(self.data.shape))
        # df_toc_statis_match[
        #     ['dis_province', 'dis_city', 'start_province', 'start_city', 'date', 'sku_id', 'qty']].to_csv(
        #     '../data/goods_layout_{}/toc_statistic_match.csv'.format(goods_layout))
        # df_toc_statis_match_na = df_toc_statis_match[df_toc_statis_match['end_city'].isna()]

    # get sales quantity of fixed cities(list) and sku(string)
    def get_sales(self, store_city, storeORshop='store_city', sku_list=None, start_date=None, end_date=None):
        if start_date is None:
            start_date = self.data["date"].min()

        if end_date is None:
            end_date = self.data["date"].max()

        # date_range = [ x.strftime('%Y%m%d') for x in pd.date_range(start_date, end_date).tolist()]
        date_range = [x.strftime('%Y-%m-%d') for x in pd.date_range(start_date, end_date).tolist()]
        date_sale = pd.DataFrame(index=date_range, dtype=int)
        sales_sub = self.data[(self.data[storeORshop] == store_city)]
        store_sku_list = list(sales_sub["sku_id"].drop_duplicates().values)
        if sku_list is None:
            for sku in store_sku_list:
                date_sale[sku] = sales_sub[sales_sub['sku_id'] == sku][['qty', 'date']].set_index('date')
        else:
            for sku in sku_list:
                date_sale[sku] = sales_sub[sales_sub['sku_id'] == sku][['qty', 'date']].set_index('date')
        return date_sale

    def get_sku(self, store_city=None, storeORshop='store_city'):

        if store_city is None:
            return list(self.data['sku_id'].drop_duplicates().values)
        else:
            return list(self.data[self.data[storeORshop] == store_city]['sku_id'].drop_duplicates().values)

    def get_stores(self, storeORshop='store_city'):

        return list(self.data[storeORshop].drop_duplicates().tolist())

    def get_daterange(self, store_city=None, storeORshop='store_city'):

        if store_city is None:
            return list(self.data['date'].drop_duplicates().values)
        else:
            return list(self.data[self.data[storeORshop] == store_city]['date'].drop_duplicates().values)


def main_flow(plan_name, area_name):
    def count_notzero_rate(series):

        not_zero = series[series > 0]
        rate = (len(not_zero) / len(series)) * 1.00
        return rate

    print('商品信息表       sku_info_df read over!')
    skudata = sku_info_1.skuData(file_path='../data/{}/sku信息：{}.xlsx'.format(plan_name, area_name))

    RDC_period = 7
    FDC_period = 2
    service_level = 0.985
    rdc_bp = 20
    fdc_bp = 5
    nrtk = 1.0
    shop_period = 0
    shop_service_level = 0
    shop_bp = 0
    shop_nrtk = 0
    parameter = {'RDC补货周期': RDC_period, 'FDC调拨周期': FDC_period, '服务水平': service_level, 'RDC_BP': rdc_bp,
                 'FDC_BP': fdc_bp, 'nrtk': nrtk,
                 'shop调拨周期': shop_period, 'shop服务水平': shop_service_level, 'shop_BP': shop_bp, 'shop_nrtk': shop_nrtk}
    # mlk
    duration_info = duration.dutation(file_path='../data/{}/仓网门店数据1：CDC-RDC.xlsx'.format(plan_name))
    CDC_RDC_info = duration_info.cdc_rdc
    RDC_FDC_info = duration_info.rdc_fdc

    RDC_list = CDC_RDC_info['RDC'].drop_duplicates().tolist()
    FAC_CDC_info = duration_info.fac_cdc
    FAC_RDC_info = duration_info.fac_rdc

    # print('商品布局方案:{}'.format(goods_layout))
    if not os.path.exists('E:/meilinkai/data/goods_layout_{}/'.format(plan_name)):
        os.mkdir('E:/meilinkai/data/goods_layout_{}/'.format(plan_name))
        os.mkdir('E:/meilinkai/data/goods_layout_{}/sale/'.format(plan_name))
        os.mkdir('E:/meilinkai/data/goods_layout_{}/stock/'.format(plan_name))
        os.mkdir('E:/meilinkai/data/goods_layout_{}/repleshment/'.format(plan_name))
    RDC_FDC_info.to_csv('../data/goods_layout_{}/RDC_FDC.csv'.format(plan_name), index=False)
    print(parameter)
    saledata_toc = SaleData('../data/{}/覆盖范围.csv'.format(plan_name),
                            '../data/{}/订单数据：{}.xlsx'.format(plan_name, area_name), plan_name, storeORshop='store_city')
    turnover_dict = {}
    shop_turnover_dict = {}
    xianhuo_day = pd.DataFrame(index=saledata_toc.get_daterange())
    xianhuo_sku = pd.DataFrame(index=saledata_toc.get_sku())
    accuracy_sku = pd.DataFrame(index=saledata_toc.get_sku())
    # shopdata_toc = SaleData('../data/goods_layout_{}/shop_toc_statistic_match.csv'.format(goods_layout),
    #                         storeORshop='shop_id')
    # shop_xianhuo_day = pd.DataFrame(index=shopdata_toc.get_daterange())
    # shop_xianhuo_sku = pd.DataFrame(index=shopdata_toc.get_sku())
    # shop_accuracy_sku = pd.DataFrame(index=shopdata_toc.get_sku())
    eclp_stock_sum = 0
    sale_sum = 0
    stock_sum = 0

    data_cdc_hangzhou_toc = saledata_toc.get_sales('杭州', storeORshop='store_city').fillna(0)
    # sale_sum += data_cdc_hangzhou_toc.values.sum()
    # print(sale_sum)
    cdc_guangzhou_sale = pd.DataFrame(index=data_cdc_hangzhou_toc.index)

    for RDC in RDC_list:
        data_rdc_toc = saledata_toc.get_sales(RDC, storeORshop='store_city').fillna(0)
        sale_sum += data_rdc_toc.values.sum()
        # print(sale_sum)
        rdc_sale = data_rdc_toc.copy()
        print('RDC：{} 销售统计！时间:{}'.format(RDC, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        if RDC in RDC_FDC_info['RDC'].drop_duplicates().tolist():
            for fdc_dic in RDC_FDC_info[RDC_FDC_info['RDC'] == RDC][['FDC', 'vlt(hour)']].to_dict('records'):
                data_toc_1 = saledata_toc.get_sales(fdc_dic['FDC'], storeORshop='store_city').fillna(0)
                print('>>>>>>>{}对应FDC:{}，模拟补货！时间：{}'.format(RDC, fdc_dic['FDC'],
                                                            datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                stock_toc_1 = pd.DataFrame(index=data_toc_1.index)
                replenish_toc_1 = pd.DataFrame(index=data_toc_1.index)

                for sku in saledata_toc.get_sku(fdc_dic['FDC'], storeORshop='store_city'):
                    if skudata.get_factory(sku) == '启东':
                        print(fdc_dic['FDC'] + sku + '危险品')
                    stock_toc_1[sku], replenish_toc_1[sku], accuracy_sku.loc[
                        sku, fdc_dic['FDC']] = inventoryCal.stock_simulation_2(data_toc_1[sku], FDC_period,
                                                                               math.ceil(fdc_dic['vlt(hour)'] / 24),
                                                                               k=service_level, bp=fdc_bp, nrtk=nrtk,
                                                                               min_except=1)
                stock_sum += stock_toc_1.values.sum()
                sale_sum += data_toc_1.values.sum()
                print(stock_sum, sale_sum)
                turnover_dict[fdc_dic['FDC']] = stock_toc_1.fillna(0).values.sum() / data_toc_1.fillna(0).values.sum()
                data_toc_1.to_csv('../data/goods_layout_{}/sale/FDC_{}.csv'.format(plan_name, fdc_dic['FDC']))
                stock_toc_1.to_csv('../data/goods_layout_{}/stock/FDC_{}.csv'.format(plan_name, fdc_dic['FDC']))
                replenish_toc_1.to_csv(
                    '../data/goods_layout_{}/repleshment/FDC_{}.csv'.format(plan_name, fdc_dic['FDC']))
                print(
                    ' >>>>>>>{}对应FDC:{}计算完毕！平均周转天数为：{:.2f}'.format(RDC, fdc_dic['FDC'], turnover_dict[fdc_dic['FDC']]))
                xianhuo_day[fdc_dic['FDC']] = stock_toc_1.apply(count_notzero_rate, axis=1)
                xianhuo_sku[fdc_dic['FDC']] = stock_toc_1.apply(count_notzero_rate, axis=0)
                rdc_sale = rdc_sale.add(replenish_toc_1, fill_value=0)
        rdc_sale = rdc_sale.fillna(0)
        stock_df = pd.DataFrame(index=rdc_sale.index)
        replenish_df = pd.DataFrame(index=rdc_sale.index)
        print('RDC：{} 正在模拟库存！'.format(RDC))
        for sku in list(rdc_sale.columns):
            if RDC != '杭州市' and area_name == '华中':
                vlt = CDC_RDC_info.set_index(['总仓', 'RDC']).loc[('杭州市', RDC), 'vlt(hour)']
                stock_df[sku], replenish_df[sku], accuracy_sku.loc[sku, RDC] = inventoryCal.stock_simulation_2(
                    rdc_sale[sku], RDC_period, math.ceil(vlt / 24),
                    k=service_level, bp=rdc_bp, nrtk=nrtk, min_except=1)
                cdc_guangzhou_sale = cdc_guangzhou_sale.add(replenish_df[[sku]], fill_value=0)
            else:
                vlt = CDC_RDC_info.set_index(['总仓', 'RDC']).loc[('杭州市', RDC), 'vlt(hour)']
                stock_df[sku], replenish_df[sku], accuracy_sku.loc[sku, RDC] = inventoryCal.stock_simulation_2(
                    rdc_sale[sku], RDC_period, math.ceil(vlt / 24),
                    k=service_level, bp=rdc_bp, nrtk=nrtk, min_except=1)
        print("开始保存")
        rdc_sale.to_csv('../data/goods_layout_{}/sale/RDC_{}.csv'.format(plan_name, RDC))
        stock_df.to_csv('../data/goods_layout_{}/stock/RDC_{}.csv'.format(plan_name, RDC))
        replenish_df.to_csv('../data/goods_layout_{}/repleshment/RDC_{}.csv'.format(plan_name, RDC))
        stock_sum += int(stock_df.fillna(0).values.sum())
        if RDC_FDC_info.empty == False:
            sale_sum += rdc_sale.values.sum()
        print(stock_df.fillna(0).values.sum(), rdc_sale.fillna(0).values.sum())
        turnover_dict[RDC] = stock_df.fillna(0).values.sum() / rdc_sale.fillna(0).values.sum()
        xianhuo_day[RDC] = stock_df.apply(count_notzero_rate, axis=1)
        xianhuo_sku[RDC] = stock_df.apply(count_notzero_rate, axis=0)
        print('{}模拟完成！平均周转天数为：{:.2f}'.format(RDC, turnover_dict[RDC]))
        del stock_df
        del replenish_df
    # cdc_guangzhou_sale = cdc_guangzhou_sale.fillna(0)
    # cdc_wuhan_sale = cdc_wuhan_sale.fillna(0)
    cdc_guangzhou_stock_df = pd.DataFrame(index=cdc_guangzhou_sale.index)
    # cdc_wuhan_stock_df = pd.DataFrame(index=cdc_wuhan_sale.index)
    cdc_guangzhou_replenish_df = pd.DataFrame(index=cdc_guangzhou_sale.index)
    # cdc_wuhan_replenish_df = pd.DataFrame(index=cdc_wuhan_sale.index)
    # print('CDC 武汉 正在模拟库存！')
    # cdc_wuhan_sale_use = pd.DataFrame(index=cdc_wuhan_sale.index)
    # for sku in list(cdc_wuhan_sale.columns):
    #     cdc_wuhan_sale_use[sku] = cdc_wuhan_sale[sku]
    #     cdc_wuhan_stock_df[sku], cdc_wuhan_replenish_df[sku], accuracy_sku.loc[
    #         sku, 'CDC武汉'] = inventoryCal.stock_simulation_2(cdc_wuhan_sale[sku],
    #                                                         RDC_period,
    #                                                         math.ceil(
    #                                                             42 / 24),
    #                                                         k=service_level,
    #                                                         bp=bp, nrtk=nrtk, min_except=1)
    #
    # cdc_wuhan_sale_use.dropna().to_csv('../data/goods_layout_{}/sale/CDC_{}.csv'.format(goods_layout, '武汉'))
    # cdc_wuhan_stock_df.to_csv('../data/goods_layout_{}/stock/CDC_{}.csv'.format(goods_layout, '武汉'))
    # cdc_wuhan_replenish_df.to_csv('../data/goods_layout_{}/repleshment/CDC_{}.csv'.format(goods_layout, '武汉'))
    # stock_sum += cdc_wuhan_stock_df.values.sum()
    # # sale_sum += cdc_wuhan_sale_use.values.sum()
    # turnover_dict['cdc武汉'] = cdc_wuhan_stock_df.values.sum() / cdc_wuhan_sale_use.values.sum()
    # print('{}模拟完成！平均周转天数为：{:.2f}'.format('cdc武汉', turnover_dict['cdc武汉']))
    if cdc_guangzhou_sale.empty == False:
        print('CDC 杭州市 正在模拟库存！')
        cdc_guangzhou_sale_use = pd.DataFrame(index=cdc_guangzhou_sale.index)
        for sku in list(cdc_guangzhou_sale.columns):
            vlt = FAC_CDC_info.set_index(['工厂', 'CDC']).loc[(skudata.get_factory(sku) + '市', '杭州市'), 'vlt(hour)']
            cdc_guangzhou_sale_use[sku] = cdc_guangzhou_sale[sku]
            cdc_guangzhou_stock_df[sku], cdc_guangzhou_replenish_df[sku], accuracy_sku.loc[
                sku, 'CDC杭州市'] = inventoryCal.stock_simulation_2(cdc_guangzhou_sale[sku],
                                                                 RDC_period,
                                                                 math.ceil(
                                                                     vlt / 24),
                                                                 k=service_level,
                                                                 bp=rdc_bp, nrtk=nrtk, min_except=1)

        cdc_guangzhou_sale_use.dropna().to_csv('../data/goods_layout_{}/sale/CDC_{}.csv'.format(plan_name, '杭州市'))
        cdc_guangzhou_stock_df.to_csv('../data/goods_layout_{}/stock/CDC_{}.csv'.format(plan_name, '杭州市'))
        cdc_guangzhou_replenish_df.to_csv('../data/goods_layout_{}/repleshment/CDC_{}.csv'.format(plan_name, '杭州市'))

        stock_sum += cdc_guangzhou_stock_df.values.sum()
        sale_sum += cdc_guangzhou_sale_use.values.sum()

        turnover_dict['cdc杭州市'] = cdc_guangzhou_stock_df.values.sum() / cdc_guangzhou_sale_use.values.sum()
        print('{}模拟完成！平均周转天数为：{:.2f}'.format('cdc杭州市', turnover_dict['cdc杭州市']))
        print(stock_sum, sale_sum)
        turnover_dict['总体'] = stock_sum / sale_sum
    else:
        print(stock_sum, sale_sum)
        turnover_dict['总体'] = stock_sum / sale_sum
    writer = pd.ExcelWriter(
        '../data/goods_layout_{}/RDC_BP{}FDC_BP{}补货{}调拨{}服务水平{}NRTk{}.xlsx'.format(plan_name, rdc_bp, fdc_bp,
                                                                                   RDC_period, FDC_period,
                                                                                   service_level, nrtk))
    pd.DataFrame(parameter, index=['0', ]).to_excel(writer, sheet_name='参数详情')
    xianhuo_day.to_excel(writer, sheet_name='现货率按天')
    xianhuo_sku.to_excel(writer, sheet_name='现货率按SKU')
    accuracy_sku.to_excel(writer, sheet_name='预测准确率按SKU')
    # shop_xianhuo_day.to_excel(writer, sheet_name='门店现货率按天')
    # shop_xianhuo_sku.to_excel(writer, sheet_name='门店现货率按SKU')
    # shop_accuracy_sku.to_excel(writer, sheet_name='门店预测准确率按SKU')
    pd.DataFrame.from_dict(turnover_dict, orient='index').to_excel(writer, sheet_name='仓库平均周转天数')
    pd.DataFrame.from_dict(shop_turnover_dict, orient='index').to_excel(writer, sheet_name='门店平均周转天数')
    print('模拟完成！！！时间:{}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    # percent_dir1 = r'../data/cal_data/result/eclp/result{}'.format(percent)
    # split_pallet_num1 = 2.0
    # anli = AnliSimulation.AnliSimulation(percent_dir=percent_dir1, split_pallet_num=split_pallet_num1)
    # anli.cal_storage_cost().to_excel(writer,sheet_name='仓储费用')
    # anli.cal_transport_cost_1().to_excel(writer,sheet_name='补货以及调拨费用')
    writer.save()
