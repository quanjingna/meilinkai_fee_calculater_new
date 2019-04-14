

import pandas as pd

class dutation:
    def __init__(self,file_path):
        self.cdc_rdc = pd.read_excel(file_path, sheet_name='总仓-RDC')
        self.cdc_rdc = self.cdc_rdc.drop_duplicates()
        print('总仓-RDC     read over!')
        self.fac_cdc = pd.read_excel(file_path, sheet_name='工厂-CDC')
        self.fac_cdc = self.fac_cdc.drop_duplicates()
        print('工厂-CDC     read over!')
        self.fac_rdc = pd.read_excel(file_path, sheet_name='工厂-RDC')
        self.fac_rdc = self.fac_rdc.drop_duplicates()
        print('工厂-RDC     read over!')
        self.rdc_fdc = pd.read_excel(file_path, sheet_name='RDC-FDC')
        self.rdc_fdc = self.rdc_fdc.drop_duplicates()
        print('RDC-FDC      read over!')


if __name__ == '__main__':
    plan_name = '1、东北：杭州-沈阳'
    duration_info = dutation(file_path='../data/{}/仓网门店数据1：CDC-RDC.xlsx'.format(plan_name))
    CDC_RDC_info = duration_info.cdc_rdc
    RDC_FDC_info = duration_info.rdc_fdc
    RDC_list = CDC_RDC_info['RDC'].drop_duplicates().tolist()
    FAC_CDC_info = duration_info.fac_cdc
    FAC_RDC_info = duration_info.fac_rdc