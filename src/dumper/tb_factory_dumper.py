from ..reqs import TBFactoryReqs

import time
import subprocess as sub

class TBFactoryDumper(object):
    def __init__(self, cookie: str) -> None:
        self._cookie = cookie
        self._reqs = TBFactoryReqs(cookie=cookie)

    def dump_all(self, pay_date_start: str, pay_date_end: str,
                 apply_date_start: str, apply_date_end: str,
                 bill_date_start: str, bill_date_end: str,
                 ad_charge_date_start: str, ad_charge_date_end: str):
        self.dumpDetail(TBFactoryReqs.BILLTYPE_ADVERTISING_CHARGE, ad_charge_date_start, ad_charge_date_end)
        self.dumpRefundOrders(apply_date_start, apply_date_end)
        self.dumpOrders(pay_date_start, pay_date_end)
        self.dumpSettleBill(bill_date_start, bill_date_end)
        self.dumpDetail(TBFactoryReqs.BILLTYPE_SUPPLIER_MARKETING, bill_date_start, bill_date_end)
        self.dumpDetail(TBFactoryReqs.BILLTYPE_FREIGHT_INSURANCE, bill_date_start, bill_date_end)
        self.dumpDetail(TBFactoryReqs.BILLTYPE_ALL_SITE_CHANNEL_PROMOTION, bill_date_start, bill_date_end)
        self.dumpDetail(TBFactoryReqs.BILLTYPE_CREDIT_BUY, bill_date_start, bill_date_end)

    def dumpRefundOrders(self, apply_date_start: str, apply_date_end: str) -> str:
        # 导出
        id = self._reqs.exportRefundOrder(apply_date_start=apply_date_start, apply_date_end=apply_date_end)
        print('export refund orders from {} to {}, task id: {}'.format(apply_date_start, apply_date_end, id))
        if id is None:
            sub.check_call('cp resource/empty_files/refund_orders.xlsx output', shell=True)
            return 'output/refund_orders.xlsx'
        time.sleep(3)
        download_url = self._reqs.querySingleRefundOrderRecord(id)
        # 下载
        #return self._reqs.download(download_url, 'output/refund_orders.xlsx')
        return self._reqs.download_noheaders(download_url, 'output/refund_orders.xlsx')


    def dumpOrders(self, pay_date_start: str, pay_date_end: str) -> str:
        # 导出
        id = self._reqs.exportOrder(order_pay_date_start=pay_date_start, order_pay_date_end=pay_date_end)
        print('export orders from {} to {}, export id: {}'.format(pay_date_start, pay_date_end, id))
        if id is None:
            sub.check_call('cp resource/empty_files/orders.xlsx output', shell=True)
            return 'output/orders.xlsx'
        time.sleep(3)
        id_records = self._reqs.queryExportOrderTaskRecords({id})
        download_url = id_records[id]
        # 下载
        return self._reqs.download(download_url, 'output/orders.xlsx')

    def dumpSettleBill(self, bill_date_start: str, bill_date_end: str) -> str:
        # 导出
        id = self._reqs.exportSettleBill(bill_date_start=bill_date_start, bill_date_end=bill_date_end)
        print('export settle bill from {} to {}, export id: {}'.format(bill_date_start, bill_date_end, id))
        if id is None:
            sub.check_call('cp resource/empty_files/settle_bill.xlsx output', shell=True)
            return 'output/settle_bill.xlsx'
        time.sleep(3)
        download_url = self._reqs.querySingleExportSettleBillRecord(id)
        # 下载
        filepath = 'output/settle_bill.zip'
        unzip_filepath = 'output/settle_bill'
        target_filepath = 'output/settle_bill.xlsx'
        self._reqs.download(download_url, filepath)
        sub.check_call('unzip {filepath} -d {unzip_filepath} && mv {unzip_filepath}/* {target_filepath} && rm -rf {unzip_filepath} && rm -f {filepath}'.format(
            filepath=filepath, unzip_filepath=unzip_filepath, target_filepath=target_filepath), shell=True)
        return target_filepath
    
    def dumpDetail(self, billtype: str, bill_date_start: str, bill_date_end: str) -> str:
        # 导出
        id = self._reqs.exportDetail(billtype, bill_date_start=bill_date_start, bill_date_end=bill_date_end)
        print('export detail: {}, from {} to {}, export id: {}'.format(billtype, bill_date_start, bill_date_end, id))
        if id is None:
            sub.check_call('cp resource/empty_files/{}.xlsx output'.format(billtype), shell=True)
            return 'output/{}.xlsx'.format(billtype)
        time.sleep(3)
        download_url = self._reqs.querySingleExportDetailRecord(id)
        # 下载
        filepath = 'output/{}.zip'.format(billtype)
        unzip_filepath = 'output/{}'.format(billtype)
        target_filepath = 'output/{}.xlsx'.format(billtype)
        self._reqs.download(download_url, filepath)
        sub.check_call('unzip {filepath} -d {unzip_filepath} && mv {unzip_filepath}/* {target_filepath} && rm -rf {unzip_filepath} && rm -f {filepath}'.format(
            filepath=filepath, unzip_filepath=unzip_filepath, target_filepath=target_filepath), shell=True)
        return target_filepath
        
