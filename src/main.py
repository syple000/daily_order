from .conf import Conf
from .dumper import TBFactoryDumper
from .report import Reporter
from .summary import Summary
import subprocess as sub

if __name__ == '__main__':
    #sub.check_call('rm -rf output && mkdir -p output', shell=True)
    #TBFactoryDumper(Conf('conf.json').load(['cookie'])).dump_all('2024-09-01', '2024-10-11',
    #                                            '2024-09-01', '2024-10-11')
    df = Reporter().report()
    df.to_csv('report.csv', index=False)
    #Summary(df).dumpLinkSkuTable('resource/dump_link_sku_table.csv')
    df = Summary(df).calcProfit('resource/link_sku_table.csv')
    df.to_csv('profit.csv', index=False)