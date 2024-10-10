from .conf import Conf
from .reqs import TBFactoryReqs
import time

if __name__ == '__main__':
    tb_fac_reqs = TBFactoryReqs(Conf().load(['cookie']))
    #export_order_id = tb_fac_reqs.exportOrder('2024-10-01', '2024-10-10')
    #time.sleep(10)
    #download_urls = tb_fac_reqs.queryExportOrderTaskRecords({export_order_id})
    #tb_fac_reqs.download(download_urls[export_order_id], 'test.xlsx')
    #object_id = tb_fac_reqs.exportSettleBill('2024-10-01', '2024-10-10')
    object_link = tb_fac_reqs.querySingleExportSettleBillRecord(object_id='YWVNTHRwemJRNmJsRlN0bk5xMDM2cFdNTWdGb211VEV4bjloVndnZVd3c2wycXE0WW9NOVpPbHJmQ2pkZUpBZ0VrcHZhN3pVSnJSTWcxcmpmNnJxelRvblFOUVViNHU5UzhJLzVITzhxS1E9')
    tb_fac_reqs.download(object_link, 'test_object.zip')