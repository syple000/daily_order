import datetime
import requests
import json
from typing import Dict, Set

from ..utils import retry

# 如果需要结合历史做到查重不重不漏，需要多查去重
# 淘宝商家页面非必要传参乱，实现比较乱。网页下载是轮询实现
class TBFactoryReqs(object):
    BROWSER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'

    BILLTYPE_SUPPLIER_MARKETING = 'SUPPLIER_MARKETING' # 营销推广套餐佣金
    BILLTYPE_ADVERTISING_CHARGE = 'ADVERTISING_CHARGE' # 广告代投账单
    BILLTYPE_ALL_SITE_CHANNEL_PROMOTION = 'ALL_SITE_CHANNEL_PROMOTION' # 商品广告推广
    BILLTYPE_CREDIT_BUY = 'CREDIT_BUY' # 先用后付
    BILLTYPE_FREIGHT_INSURANCE = 'FREIGHT_INSURANCE' # 运费险

    def __init__(self, cookie: str) -> None:
        self._cookie = cookie

    # 退款导出 & 查询
    @retry()
    def exportRefundOrder(self, apply_date_start: str, apply_date_end: str) -> str: # 返回任务id
        start = datetime.datetime.strptime(apply_date_start, '%Y-%m-%d')
        end = datetime.datetime.strptime(apply_date_end, '%Y-%m-%d') + datetime.timedelta(days=1)
        body = {
            'refundApplyStartTime': start.strftime('%Y-%m-%d %H:%M:%S'),
            'refundApplyEndTime': (end + datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),
        }
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cookie': self._cookie,
            'user-agent': TBFactoryReqs.BROWSER_AGENT,
            'content-type': 'application/json;charset=UTF-8', 
        }
        resp = requests.post('https://tgc.tmall.com/ds/api/v1/supplier/refund/gei/exportServeDatas', headers=headers, data=json.dumps(body))
        if not resp.ok:
            raise Exception('req export refund order status code: {}'.format(resp.status_code))
        resp_json = resp.json()
        if resp_json['success']:
            print('export refund order data: {}'.format(resp_json['data']))
            return resp_json['data']
        if '导出数量必须大于0' in resp_json['errorMessage']:
            return None
        raise Exception('resp fail: {}'.format(json.dumps(resp_json)))

    @retry()
    def querySingleRefundOrderRecord(self, task_id: str) -> str: # 返回下载链接
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cookie': self._cookie,
            'user-agent': TBFactoryReqs.BROWSER_AGENT,
        }
        params = {
            'taskId': task_id,
        }
        resp = requests.get('https://tgc.tmall.com/ds/api/v1/supplier/refund/gei/queryExportProgress', headers=headers, params=params)
        if not resp.ok:
            raise Exception('query single refund order status code: {}'.format(resp.status_code))
        resp_json = resp.json()
        if resp_json['success']:
            if resp_json['data']['download'] is None:
                raise Exception('query single export refund order null')
            print('query single export refund order data: {}'.format(resp_json['data']['download']))
            return resp_json['data']['download']
        raise Exception('query single export refund order not found: {}'.format(task_id))
 
    
    # 明细（活动等）导出 & 查询
    @retry()
    def exportDetail(self, billtype: str, bill_date_start: str, bill_date_end: str) -> str: # 返回对象标识
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cookie': self._cookie,
            'user-agent': TBFactoryReqs.BROWSER_AGENT,
            'content-type': 'application/json;charset=UTF-8', 
        }
        body = {
            'billType': billtype,
            "billDateStart": bill_date_start,
            "billDateEnd": bill_date_end,
        }
        resp = requests.post('https://tgc.tmall.com/ds/api/v1/finance/bill/common/export', headers=headers, data=json.dumps(body))
        if not resp.ok:
            raise Exception('req export detail status code: {}'.format(resp.status_code))
        resp_json = resp.json()
        if resp_json['success']:
            print('export detail data {}: {}'.format(billtype, resp_json['data']['objectName']))
            return resp_json['data']['objectName']
        if '导出数量必须大于0' in resp_json['errorMessage']:
            return None
        raise Exception('resp fail: {}'.format(json.dumps(resp_json)))

    @retry()
    def querySingleExportDetailRecord(self, object_id: str) -> str: # 返回下载链接
        headers = {
            'accept': 'application/json, text/json',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cookie': self._cookie,
            'user-agent': TBFactoryReqs.BROWSER_AGENT,
            'content-type': 'application/json', 
        }
        body = {
            'key': object_id,
        }
        resp = requests.post('https://tgc.tmall.com/ds/api/v1/finance/bill/getExportFileUrl', headers=headers, data=json.dumps(body))
        if not resp.ok:
            raise Exception('query single export detail status code: {}'.format(resp.status_code))
        resp_json = resp.json()
        if resp_json['success']:
            if resp_json['data'] is None:
                raise Exception('query single export detail null')
            print('query single export detail data: {}'.format(resp_json['data']))
            return resp_json['data']
        raise Exception('query single export detail not found: {}'.format(object_id))
 

    # 结算导出 & 查询
    @retry()
    def exportSettleBill(self, bill_date_start: str, bill_date_end: str) -> str: # 返回对象标识
        headers = {
            'accept': 'application/json, text/json',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cookie': self._cookie,
            'user-agent': TBFactoryReqs.BROWSER_AGENT,
            'content-type': 'application/json', 
        }
        body = {
            'bizCode': 'UNIFY_BILL',
            'condition': {
                'billPeriod': {
                    'endDate': bill_date_end,
                    'startDate': bill_date_start,
                }
            }
        }
        resp = requests.post('https://tgc.tmall.com/ds/api/v1/cost-shared/download', headers=headers, data=json.dumps(body))
        if not resp.ok:
            raise Exception('req export settle bill status code: {}'.format(resp.status_code))
        resp_json = resp.json()
        if resp_json['success']:
            print('export settle bill data: {}'.format(resp_json['data']['objectName']))
            return resp_json['data']['objectName']
        if '导出数量必须大于0' in resp_json['errorMessage']:
            return None
        raise Exception('resp fail: {}'.format(json.dumps(resp_json)))

    @retry()
    def querySingleExportSettleBillRecord(self, object_id: str) -> None|str: # 返回下载链接
        params = {
            'objectName': object_id,
        }
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cookie': self._cookie,
            'user-agent': TBFactoryReqs.BROWSER_AGENT,
        }
        resp = requests.get('https://tgc.tmall.com/ds/api/v1/cost-shared/download-polling', params=params, headers=headers)
        if not resp.ok:
            raise Exception('query single export settle bill status code: {}'.format(resp.status_code))
        resp_json = resp.json()
        if resp_json['success']:
            if resp_json['data'] is None:
                raise Exception('query single export settle bill null')
            print('query single export settle bill data: {}'.format(resp_json['data']))
            return resp_json['data']
        raise Exception('query single export settle bill not found: {}'.format(object_id))

 
    # 订单导出 & 查询

    @retry()
    def exportOrder(self, order_pay_date_start: str, order_pay_date_end: str) -> int: # 返回任务id
        start = datetime.datetime.strptime(order_pay_date_start, '%Y-%m-%d')
        end = datetime.datetime.strptime(order_pay_date_end, '%Y-%m-%d') + datetime.timedelta(days=1)
        params = {
            'payTimeStart': int(datetime.datetime.timestamp(start)) * 1000,
            'payTimeEnd': int(datetime.datetime.timestamp(end)) * 1000,
        }
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cookie': self._cookie,
            'user-agent': TBFactoryReqs.BROWSER_AGENT
        }
        resp = requests.get('https://tgc.tmall.com/api/v1/orderNew/createPanamaOrderExportTask.htm', params=params, headers=headers)
        if not resp.ok:
            raise Exception('req export order status code: {}'.format(resp.status_code))
        resp_json = resp.json()
        if resp_json['success']:
            print('export order data: {}'.format(resp_json['data']))
            return int(resp_json['data'])
        if '导出数量必须大于0' in resp_json['errorMessage']:
            return None
        raise Exception('resp fail: {}'.format(json.dumps(resp_json)))
    
    @retry()
    def queryExportOrderTaskRecords(self, task_ids: Set[int]) -> Dict[int, None|str]: # 根据任务id查找任务结果下载链接（最近100条），任务还在执行返回None
        params = {
            'pageNo': 1,
            'pageSize': 100
        }
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cookie': self._cookie,
            'user-agent': TBFactoryReqs.BROWSER_AGENT,
        }
        resp = requests.get('https://tgc.tmall.com/api/v1/orderNew/taskRecordList.htm', params=params, headers=headers)
        if not resp.ok:
            raise Exception('query export order task records status code: {}'.format(resp.status_code))
        resp_json = resp.json()
        id_url_map = {}
        for record in resp_json['data']['data']:
            id = int(record['id'])
            if id not in task_ids:
                continue
            if record['status'] == '导出处理中':
                raise Exception('query export orders null')
            if record['status'] == '导出成功' and int(record['percent']) == 100 and int(record['errorCount']) == 0:
                print('task record: {}, {}'.format(id, record['fileDownList'][0]['downUrl']))
                id_url_map[id] = record['fileDownList'][0]['downUrl']
            if len(id_url_map) == len(task_ids):
                print('query export order task records: {}'.format(json.dumps(id_url_map)))
                return id_url_map
        raise Exception('req task record not found: {}, {}'.format(task_ids, id_url_map))

    # 下载

    @retry()
    def download(self, url: str, filepath: str) -> str:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cookie': self._cookie,
            'upgrade-insecure-requests': '1',
            'user-agent': TBFactoryReqs.BROWSER_AGENT,
        }
        resp = requests.get(url=url, headers=headers, timeout=10)
        if not resp.ok:
            raise Exception('download status code: {}'.format(resp.status_code))
        with open(filepath, 'wb') as f:
            f.write(resp.content)
        return filepath

    @retry()
    def download_noheaders(self, url: str, filepath: str) -> str:
        resp = requests.get(url=url, timeout=10)
        if not resp.ok:
            raise Exception('download no headers status code: {}'.format(resp.status_code))
        with open(filepath, 'wb') as f:
            f.write(resp.content)
        return filepath

