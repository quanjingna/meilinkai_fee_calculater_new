# -*- coding: utf-8 -*-
"""
This class is for reading and getting data from sale_data.csv
example：
1. initializing xxx = SaleData(file_path)  # file_path is the path of sale_data.csv
2. get sales quantity of fixed cities(list) and sku(string). You can give start_date and end_date(optional)
    eg: xxx.get_sales(cities=["city1", "city2"], sku="123456")
"""
import pandas as pd
import inventoryCal
import AnliSimulation
import math
from datetime import datetime


class SaleData:
    # initializing and read csv, default encoding is utf-8
    def __init__(self, file_path, sku_list=None,storeORshop='store_city', encoding="utf-8"):

        self.data = pd.read_csv(file_path, encoding=encoding, usecols = ['sku_id','date','qty',storeORshop],dtype=
                               {'sku_id':str,'qty':float,'date':str},low_memory=False).dropna()
        self.data['sku_id'] = self.data['sku_id'].apply(lambda x: x.strip())
        self.data = self.data.groupby([storeORshop,'sku_id','date'],as_index=False).agg(sum)
        if sku_list is not None:
            self.data = self.data[self.data["sku_id"].isin(sku_list)]
            self.sku_list = sku_list
        else:
            self.sku_list = list(self.data["sku_id"].drop_duplicates().values)

    # get sales quantity of fixed cities(list) and sku(string)
    def get_sales(self, store_city, storeORshop='store_city' ,sku_list=None, start_date=None, end_date=None):
        if start_date is None:
            start_date = self.data["date"].min()

        if end_date is None:
            end_date = self.data["date"].max()

        # date_range = [ x.strftime('%Y%m%d') for x in pd.date_range(start_date, end_date).tolist()]
        date_range = [ x.strftime('%Y-%m-%d') for x in pd.date_range(start_date, end_date).tolist()]
        date_sale = pd.DataFrame(index=date_range,dtype=int)
        sales_sub = self.data[(self.data[storeORshop] == store_city)]
        store_sku_list = list(sales_sub["sku_id"].drop_duplicates().values)
        if sku_list is None:
            for sku in store_sku_list:
                date_sale[sku] = sales_sub[sales_sub['sku_id']==sku][['qty','date']].set_index('date')
        else:
            for sku in sku_list:
                date_sale[sku] = sales_sub[sales_sub['sku_id']==sku][['qty','date']].set_index('date')
        return date_sale

    def get_sku(self,store_city=None,storeORshop='store_city'):

        if store_city is None:
            return list(self.data['sku_id'].drop_duplicates().values)
        else:
            return list(self.data[self.data[storeORshop] == store_city]['sku_id'].drop_duplicates().values)

    def get_stores(self,storeORshop='store_city'):

            return list(self.data[storeORshop].drop_duplicates().tolist())

    def get_daterange(self,store_city=None,storeORshop='store_city'):

        if store_city is None:
            return list(self.data['date'].drop_duplicates().values)
        else:
            return list(self.data[self.data[storeORshop] == store_city]['date'].drop_duplicates().values)

if __name__ == '__main__':


    def count_notzero_rate(series):

        not_zero = series[series > 0]
        rate = (len(not_zero)/len(series))*1.00
        return rate

    goods_layout = 8
    RDC_period = 5
    FDC_period = 5
    service_level = 0.97
    bp= 10
    nrtk = 2.0
    shop_period = 3
    shop_service_level = 0.97
    shop_bp=20
    shop_nrtk = 3.0

    parameter = {'RDC补货周期': RDC_period, 'FDC调拨周期': FDC_period, '服务水平': service_level, 'BP': bp, 'nrtk': nrtk,
                 'shop调拨周期': shop_period, 'shop服务水平': shop_service_level, 'shop_BP': shop_bp, 'shop_nrtk': shop_nrtk}
    RDC_info = pd.read_csv('../data/goods_layout_{}/RDC.csv'.format(goods_layout))
    FDC_info = pd.read_csv('../data/goods_layout_{}/FDC.csv'.format(goods_layout))
    RDC_list = RDC_info['RDC'].drop_duplicates().tolist()
    shop_info = pd.read_csv('../data/goods_layout_{}/shops_match.csv'.format(goods_layout))
    sku_info = pd.read_csv('../data/sku_info_new.csv')
    XS_sku_list = sku_info[sku_info['类型']=='XS饮料']['sku_id'].drop_duplicates().tolist()
    cdc_sku_list = sku_info[(sku_info['类型']=='XS饮料')|(sku_info['类型']=='外购品')]['sku_id'].drop_duplicates().tolist()
    print('商品布局方案:{}'.format(goods_layout))
    # print('FDC补货周期:{}'.format(FDC_period))
    # print('RDC补货周期:{}'.format(RDC_period))
    # print('服务水平:{}'.format(service_level))
    # print('bp:{}'.format(bp))
    # print('nrtk:{}'.format(nrtk))
    print(parameter)
    # saledata_toc = SaleData('../data/cal_data/toc/sale_2c_splited{}.csv'.format(percent))
    saledata_toc = SaleData('../data/goods_layout_{}/toc_order_match.csv'.format(goods_layout),storeORshop='start_city')
    turnover_dict = {}
    shop_turnover_dict = {}

    xianhuo_day = pd.DataFrame(index=saledata_toc.get_daterange())
    xianhuo_sku = pd.DataFrame(index=saledata_toc.get_sku())
    accuracy_sku = pd.DataFrame(index=saledata_toc.get_sku())
    shopdata_toc = SaleData('../data/goods_layout_{}/shop_toc_statistic_match.csv'.format(goods_layout),storeORshop='shop_id')
    shop_xianhuo_day = pd.DataFrame(index=shopdata_toc.get_daterange())
    shop_xianhuo_sku = pd.DataFrame(index=shopdata_toc.get_sku())
    shop_accuracy_sku = pd.DataFrame(index=shopdata_toc.get_sku())
    eclp_stock_sum = 0
    sale_sum = 0
    stock_sum =0


    data_cdc_toc = saledata_toc.get_sales('广州', storeORshop='start_city').fillna(0)
    sale_sum += data_cdc_toc.values.sum()
    cdc_sale = pd.DataFrame(index=data_cdc_toc.index)
    for RDC in RDC_list:
    # for RDC in ['成都']:
        data_rdc_toc = saledata_toc.get_sales(RDC,storeORshop='start_city').fillna(0)
        sale_sum += data_rdc_toc.values.sum()
        rdc_sale = data_rdc_toc.copy()
        print('RDC：{} 销售统计！时间:{}'.format(RDC, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        if RDC in shop_info['RDC'].drop_duplicates().tolist():
            print('门店销售统计，并模拟计算！')
            for shop_dic in shop_info[shop_info['RDC']==RDC][['shop_id','VLT']].to_dict('records'):
                if  shop_dic['shop_id'] in shopdata_toc.get_stores(storeORshop='shop_id'):
                    data_toc_1 = shopdata_toc.get_sales(shop_dic['shop_id'],storeORshop='shop_id').fillna(0)
                    print(' -------{}对应shop:{}开始计算'.format(RDC,shop_dic['shop_id']))
                    stock_toc_1 = pd.DataFrame(index=data_toc_1.index)
                    replenish_toc_1 = pd.DataFrame(index=data_toc_1.index)
                    for sku in shopdata_toc.get_sku(shop_dic['shop_id'],storeORshop='shop_id'):
                        stock_toc_1[sku],replenish_toc_1[sku],shop_accuracy_sku.loc[sku,shop_dic['shop_id']] = inventoryCal.stock_simulation_2(data_toc_1[sku],shop_period,int(shop_dic['VLT']),
                                                                               k=shop_service_level,bp=shop_bp,nrtk=shop_nrtk)
                    stock_sum +=  stock_toc_1.values.sum()
                    sale_sum  +=  data_toc_1.values.sum()
                    shop_turnover_dict[shop_dic['shop_id']] = stock_toc_1.fillna(0).values.sum()/ data_toc_1.fillna(0).values.sum()
                    data_toc_1.to_csv('../data/goods_layout_{}/sale/shop_{}.csv'.format(goods_layout, shop_dic['shop_id']))
                    stock_toc_1.to_csv('../data/goods_layout_{}/stock/shop_{}.csv'.format(goods_layout,shop_dic['shop_id']))
                    replenish_toc_1.to_csv('../data/goods_layout_{}/repleshment/shop_{}.csv'.format(goods_layout,shop_dic['shop_id']))
                    print(' -------{}对应shop:{}计算完毕！平均周转天数为：{:.2f}'.format(RDC,shop_dic['shop_id'], shop_turnover_dict[shop_dic['shop_id']]))
                    shop_xianhuo_day[shop_dic['shop_id']] = stock_toc_1.apply(count_notzero_rate, axis=1)
                    shop_xianhuo_sku[shop_dic['shop_id']] = stock_toc_1.apply(count_notzero_rate, axis=0)
                    rdc_sale = rdc_sale.add(replenish_toc_1, fill_value=0)
        if RDC in FDC_info['RDC'].drop_duplicates().tolist():
            for fdc_dic in FDC_info[FDC_info['RDC']==RDC][['FDC','时效']].to_dict('records'):
                data_toc_1 = saledata_toc.get_sales(fdc_dic['FDC'],storeORshop='start_city').fillna(0)
                print('-------{}对应FDC:{}，模拟补货！时间：{}'.format(RDC,fdc_dic['FDC'],datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                stock_toc_1 = pd.DataFrame(index=data_toc_1.index)
                replenish_toc_1 = pd.DataFrame(index=data_toc_1.index)

                for sku in saledata_toc.get_sku(fdc_dic['FDC'],storeORshop='start_city'):
                    stock_toc_1[sku],replenish_toc_1[sku],accuracy_sku.loc[sku, fdc_dic['FDC']] = inventoryCal.stock_simulation_2(data_toc_1[sku],FDC_period,math.ceil(fdc_dic['时效']/24),
                                                                           k=service_level,bp=bp,nrtk=nrtk)
                stock_sum +=  stock_toc_1.values.sum()
                sale_sum += data_toc_1.values.sum()
                turnover_dict[fdc_dic['FDC']] = stock_toc_1.fillna(0).values.sum()/ data_toc_1.fillna(0).values.sum()
                data_toc_1.to_csv('../data/goods_layout_{}/sale/FDC_{}.csv'.format(goods_layout,fdc_dic['FDC']))
                stock_toc_1.to_csv('../data/goods_layout_{}/stock/FDC_{}.csv'.format(goods_layout,fdc_dic['FDC']))
                replenish_toc_1.to_csv('../data/goods_layout_{}/repleshment/FDC_{}.csv'.format(goods_layout,fdc_dic['FDC']))
                print(' -------{}对应FDC:{}计算完毕！平均周转天数为：{:.2f}'.format(RDC,fdc_dic['FDC'], turnover_dict[fdc_dic['FDC']]))
                xianhuo_day[fdc_dic['FDC']] = stock_toc_1.apply(count_notzero_rate, axis=1)
                xianhuo_sku[fdc_dic['FDC']] = stock_toc_1.apply(count_notzero_rate, axis=0)
                rdc_sale = rdc_sale.add(replenish_toc_1, fill_value=0)
        rdc_sale = rdc_sale.fillna(0)
        stock_df = pd.DataFrame(index=rdc_sale.index)
        replenish_df = pd.DataFrame(index=rdc_sale.index)
        # print('RDC：{} 正在模拟库存！'.format(city))
        RDC_xs_df = RDC_info[RDC_info['总仓'] == '武汉'][['RDC', '时效']].set_index('RDC')
        RDC_noxs_df = RDC_info[RDC_info['总仓'] == '广州'][['RDC', '时效']].set_index('RDC')
        for sku in list(rdc_sale.columns):
            if sku in XS_sku_list:
                RDC_df = RDC_xs_df
            else:
                RDC_df = RDC_noxs_df
            stock_df[sku],replenish_df[sku],accuracy_sku.loc[sku,RDC] = inventoryCal.stock_simulation_2(rdc_sale[sku],RDC_period,math.ceil(RDC_df.loc[RDC,'时效']/24),
                                                                            k=service_level,bp=bp,nrtk=nrtk)
        if RDC != '广州':
            cdc_sale = rdc_sale.add(replenish_df, fill_value=0)
        rdc_sale.to_csv('../data/goods_layout_{}/sale/RDC_{}.csv'.format(goods_layout,RDC))
        stock_df.to_csv('../data/goods_layout_{}/stock/RDC_{}.csv'.format(goods_layout,RDC))
        replenish_df.to_csv('../data/goods_layout_{}/repleshment/RDC_{}.csv'.format(goods_layout,RDC))
        stock_sum += stock_df.values.sum()
        sale_sum += rdc_sale.values.sum()
        turnover_dict[RDC] = stock_df.fillna(0).values.sum()/rdc_sale.fillna(0).values.sum()
        xianhuo_day[RDC] = stock_df.apply(count_notzero_rate,axis=1)
        xianhuo_sku[RDC] = stock_df.apply(count_notzero_rate,axis=0)
        print('{}模拟完成！平均周转天数为：{:.2f}'.format(RDC,turnover_dict[RDC]))
        del stock_df
        del replenish_df
    cdc_sale = cdc_sale.fillna(0)
    stock_df = pd.DataFrame(index=cdc_sale.index)
    replenish_df = pd.DataFrame(index=cdc_sale.index)
    print('CDC 正在模拟库存！')
    cdc_sale_use = pd.DataFrame(index=cdc_sale.index)
    for sku in list(cdc_sale.columns):
        if sku in cdc_sku_list:
            cdc_sale_use[sku] = cdc_sale[sku]
            stock_df[sku], replenish_df[sku], accuracy_sku.loc[sku, 'CDC广州'] = inventoryCal.stock_simulation_2(cdc_sale[sku],
                                                                                                       RDC_period,
                                                                                                       1,
                                                                                                       k=service_level,
                                                                                                       bp=bp, nrtk=nrtk)

    cdc_sale_use.dropna().to_csv('../data/goods_layout_{}/sale/CDC_{}.csv'.format(goods_layout, '广州'))
    stock_df.to_csv('../data/goods_layout_{}/stock/CDC_{}.csv'.format(goods_layout, '广州'))
    replenish_df.to_csv('../data/goods_layout_{}/repleshment/CDC_{}.csv'.format(goods_layout, '广州'))
    stock_sum += stock_df.values.sum()
    sale_sum += cdc_sale_use.values.sum()
    print('{}模拟完成！平均周转天数为：{:.2f}'.format('广州', turnover_dict['广州']))
    del stock_df
    del replenish_df
    turnover_dict['总体'] = stock_sum/sale_sum
    writer = pd.ExcelWriter('../data/goods_layout_{}/BP{}补货{}调拨{}服务水平{}NRTk{}.xlsx'.format(goods_layout,bp,RDC_period,FDC_period,service_level,nrtk))
    pd.DataFrame(parameter, index=['0', ]).to_excel(writer,sheet_name='参数详情')
    xianhuo_day.to_excel(writer,sheet_name='现货率按天')
    xianhuo_sku.to_excel(writer,sheet_name='现货率按SKU')
    accuracy_sku.to_excel(writer,sheet_name='预测准确率按SKU')
    shop_xianhuo_day.to_excel(writer,sheet_name='门店现货率按天')
    shop_xianhuo_sku.to_excel(writer,sheet_name='门店现货率按SKU')
    shop_accuracy_sku.to_excel(writer,sheet_name='门店预测准确率按SKU')
    pd.DataFrame.from_dict(turnover_dict, orient='index').to_excel(writer,sheet_name='仓库平均周转天数')
    pd.DataFrame.from_dict(shop_turnover_dict, orient='index').to_excel(writer,sheet_name='门店平均周转天数')
    print('模拟完成！！！时间:{}'.format( datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    # percent_dir1 = r'../data/cal_data/result/eclp/result{}'.format(percent)
    # split_pallet_num1 = 2.0
    # anli = AnliSimulation.AnliSimulation(percent_dir=percent_dir1, split_pallet_num=split_pallet_num1)
    # anli.cal_storage_cost().to_excel(writer,sheet_name='仓储费用')
    # anli.cal_transport_cost_1().to_excel(writer,sheet_name='补货以及调拨费用')
    writer.save()