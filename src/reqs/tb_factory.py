import datetime
import requests
import json
from typing import Mapping, Set


# 如果需要结合历史做到查重不重不漏，需要多查去重
class TBFactoryReqs(object):
    BROWSER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
    def __init__(self, cookie: str) -> None:
        self._cookie = cookie

    # 结算明细导出 & 查询

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
        raise Exception('resp fail: {}'.format(json.dumps(resp_json)))

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
            print('query single export settle bill data: {}'.format(resp_json['data']))
            return resp_json['data']
        raise Exception('query single export settle bill not found: {}'.format(object_id))

 
    # 订单导出 & 查询

    def exportOrder(self, order_pay_date_start: str, order_pay_date_end: str) -> int: # 返回任务id
        start = datetime.datetime.strptime(order_pay_date_start, '%Y-%m-%d')
        end = datetime.datetime.strptime(order_pay_date_end, '%Y-%m-%d')
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
        raise Exception('resp fail: {}'.format(json.dumps(resp_json)))
    
    def queryExportOrderTaskRecords(self, task_ids: Set[int]) -> Mapping[int, None|str]: # 根据任务id查找任务结果下载链接（最近100条），任务还在执行返回None
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
                id_url_map[id] = None
            if record['status'] == '导出成功' and int(record['percent']) == 100 and int(record['errorCount']) == 0:
                print('task record: {}, {}'.format(id, record['fileDownList'][0]['downUrl']))
                id_url_map[id] = record['fileDownList'][0]['downUrl']
            if len(id_url_map) == len(task_ids):
                print('query export order task records: {}'.format(json.dumps(id_url_map)))
                return id_url_map
        raise Exception('req task record not found: {}, {}'.format(task_ids, id_url_map))

    # 下载

    def download(self, url: str, filename: str) -> str:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cookie': self._cookie,
            'upgrade-insecure-requests': '1',
            'user-agent': TBFactoryReqs.BROWSER_AGENT,
        }
        resp = requests.get(url=url, headers=headers)
        if not resp.ok:
            raise Exception('download status code: {}'.format(resp.status_code))
        filepath = 'output/{}'.format(filename)
        with open(filepath, 'wb') as f:
            f.write(resp.content)
        return filepath
