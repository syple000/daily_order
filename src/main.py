from .conf import Conf
from .dumper import TBFactoryDumper
from .report import Reporter
from .summary import Summary
import subprocess as sub
import datetime

if __name__ == '__main__':
    sub.check_call('rm -rf output && mkdir -p output', shell=True)
    today = datetime.datetime.today()
    start_day = today + datetime.timedelta(days=-30)
    today_str = today.strftime('%Y-%m-%d')
    start_day_str = start_day.strftime('%Y-%m-%d')
    
    TBFactoryDumper(Conf('conf.json').load(['cookie'])).dump_all(start_day_str, today_str, start_day_str, today_str)
    df = Reporter().report()
    Summary(df).archive(start_day_str, today_str)
