# -*- coding: utf-8 -*-
# Created by Sun Ze on 2018/4/21

"""
This class is for reading and getting data from sku_detail.csv
example：
"""
import pandas as pd

class skuData:
    # initializing and read csv, default encoding is utf-8
    # def __init__(self, file_path='../data/cal_data/sku/sku_info.csv'):
    def __init__(self, file_path='../data/sku_used.csv'):
        if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
            self.data = pd.read_excel(file_path).rename(columns={'sku_id':'sku','产地':'factory','支/板':'num_preplate','商品毛重（公斤）':'weight'})
        else:
            self.data = pd.read_csv(file_path)
        self.data['sku'] = self.data['sku'].apply(lambda x: str(x).strip())
        self.skulist = set(self.data['sku'].values)
        self.data = self.data.set_index('sku')

    def get_weight_2C(self,sku, number=None):
        "reture kg"
        if sku not in self.skulist:
            return 0
        elif number is None:
            return self.data.loc[sku, 'weight']
        else:
            weight = self.data.loc[sku, 'weight'] * number
            return weight


    def get_plate(self, sku, numbers):
        if sku not in self.skulist:
            return 0
        else:
            number_per_plate = self.data.loc[sku, "num_preplate"]
        if number_per_plate > 0:
            return numbers/number_per_plate
        else:
            return 0

    def get_order_plate(self, sku_array):
        plates = 0
        for skui in sku_array.index:
            if skui in self.data.index:
                plates += sku_array[skui]/self.data.loc[skui, "num_preplate"]
        return round(plates)

    def get_num_plate(self, sku):

        return self.data.loc[sku, "num_preplate"]


    def get_factory(self, sku):

        try:

            return self.data.loc[sku, "factory"]

        except Exception as e:

            print(e)


    # def test(self):
    #     # print(self.data)
    #     print(self.get_plate("102791CH", 29481))
    #     tests = pd.Series([20481, 7489], index=["102791CH","104135CH"])
    #     print(self.get_order_plate(tests))


# class sku_info:


if __name__ == '__main__':

    skudata = skuData(file_path='../data/sku_used_new.csv')
    print(skudata.get_plate("WHU1007CH", 2941))
    print(skudata.get_num_plate("WHU4548CH"))


