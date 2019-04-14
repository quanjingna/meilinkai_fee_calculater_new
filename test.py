# -*- coding: utf-8 -*-
# Created by sunze  on 2018/8/27
import pandas as pd

if __name__ == '__main__':

    # df = pd.read_csv('../data/order_info/stock_intime(20180101-20180630).csv')
    # store_list  = df.drop_duplicates(subset=['store_id'])[['store_id']]
    #
    # df_new = pd.read_csv('../data/store_id--0829.csv',encoding="utf-8")
    # store_list_new = set(list(df_new['分拣点代码'].unique())+list(df_new['对应区域仓代码'].unique())+list(df_new['临时代码'].unique()))
    #
    # df_new = pd.read_csv('../data/store_id--0829.csv', encoding="utf-8")
    #
    # df_po = pd.read_csv('../data/order_info/po_warehousing(20180101-20180630).csv')
    # store_po = df_po.drop_duplicates(subset=['store_id','city'])[['store_id','city']]
    #
    # df_wto = pd.read_csv('../data/order_info/wto_warehousing(20180101-20180630).csv')
    # store_wto = df_wto.drop_duplicates(subset=['store_id','city'])[['store_id','city']]
    #
    # df_2b = pd.read_csv('../data/order_info/sale_2b(20180101-20180630).csv')
    # store_2b = df_2b.drop_duplicates(subset=['store_id','start_city'])[['store_id','start_city']]
    #
    # df_2c = pd.read_csv('../data/order_info/sale_2c(20180101-20180630).csv')
    # store_2c = df_2c.drop_duplicates(subset=['store_id','start_city'])[['store_id','start_city']]
    #

    df_eclp = pd.read_csv('../data/eclp/anli_order_20181012183730.csv', low_memory=False)
    df_eclp = df_eclp[['id', '库房名称', '收货省', '收货市', '创建时间', '订单状态', '渠道类型','goods_no', '下单数量']]
    df_eclp['date'] = df_eclp['创建时间'].apply(lambda x: x[:10])
    df_eclp['start_city'] = df_eclp['库房名称'].apply(lambda x: x.split('安利')[0])
    df_eclp = df_eclp[df_eclp['订单状态'] != '取消成功']
    df_eclp = df_eclp[df_eclp['订单状态'] != '逆向发货']
    df_eclp = df_eclp[df_eclp['订单状态'] != '拒收']
    df_eclp = df_eclp[df_eclp['订单状态'] != '逆向验收']
    df_eclp = df_eclp.rename(columns={'收货省': 'end_province', 'goods_no': 'sku', '下单数量': 'qty', '收货市': 'end_city'})
    df_eclp = df_eclp[['id', 'date','sku', 'qty', 'start_city','end_province', 'end_city','渠道类型']]
    df_toc = df_eclp[df_eclp['渠道类型']=='B2C']
    df_tob = df_eclp[df_eclp['渠道类型']=='B2B']

    print('ss')

