from .conf import Conf
from .dumper import TBFactoryDumper
from .report import Reporter
from .summary import Summary
import subprocess as sub
import datetime

if __name__ == '__main__':
    sub.check_call('rm -rf output && mkdir -p output', shell=True)

    #today = datetime.datetime.today()
    today = datetime.datetime.strptime('2024-10-13', '%Y-%m-%d')
    
    #start_day = today + datetime.timedelta(days=-30) # 订单开始时间
    start_day = datetime.datetime.strptime('2024-10-06', '%Y-%m-%d')
    
    end_day = today + datetime.timedelta(days=30) # 往后一个月
    today_str = today.strftime('%Y-%m-%d')
    start_day_str = start_day.strftime('%Y-%m-%d')
    end_day_str = end_day.strftime('%Y-%m-%d')

    TBFactoryDumper(Conf('conf.json').load(['cookie'])).dump_all(
        start_day_str, today_str, # 订单
        start_day_str, end_day_str, # 退款
        start_day_str, end_day_str, # 营销等账单
        start_day_str, today_str, # 广告充值
    )
    #Summary(df).dumpLinkSkuTable('resource/tmp.csv')
    Summary(Reporter().report(), Reporter().adCharge()).archive(start_day_str, today_str)
