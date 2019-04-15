

from sale_data_2 import main_flow
from fee_calculation_model_meilinkai import fee_main_flow
import time

# '1、东北：杭州-沈阳',
#               '2、东北：杭州-北京（北京总仓覆盖沈阳仓范围）',
#               '3、东北：杭州-北京-沈阳',
plan_names_dongbei = [
                '3、东北：杭州-北京-沈阳',
              '4、东北：杭州-北京-长春',
              '5、东北：杭州-北京-哈尔滨']
for plan_name in plan_names_dongbei:
    start_time = time.time()
    main_flow(plan_name=plan_name, area_name='东北')
    fee_main_flow(plan_name=plan_name, area_name='东北')
    end_time = time.time()
    print((end_time-start_time)/60)





# plan_names_huazhong = ['6、华中：杭州-武汉',
#               '7、华中：杭州-杭州、广州',
#               '8、华中：杭州-杭州-武汉',
#               '9、华中：杭州-杭州-南昌',
#               '10、华中：杭州-杭州-衡阳',
#               '11、华中：杭州-杭州-长沙']
# for plan_name in plan_names_huazhong:
#     main_flow(plan_name=plan_name, area_name='华中')