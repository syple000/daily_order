from ..conf import Conf

import datetime
import requests
import json
from typing import Any


class TBFactoryReqs(object):
    def __init__(self, conf: Conf) -> None:
        self._date = conf.load(['date'])
        self._cookie = conf.load(['cookie'])
        self._user_agent = conf.load(['user-agent'])

    # 淘宝商家平台比较烂，如果发现数据错误，再细查
    def createOrderExportTask(self) -> int: # 返回任务id
        date = datetime.datetime.strptime(self._date, '%Y-%m-%d')
        pre_date = date + datetime.timedelta(days=-22)
        params = {
            'sourceTradeId': None,
            'payTimeStart': int(datetime.datetime.timestamp(pre_date)) * 1000,
            'payTimeEnd': int(datetime.datetime.timestamp(date)) * 1000,
        }
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cookie': self._cookie,
            'priority': 'u=1, i',
            'referer': 'https://tgc.tmall.com/ds/page/supplier/order-manage?c2mNavigatorShellPage=0&c2mNavigatorPageOpener=',
            'user-agent': self._user_agent,
        }
        resp = requests.get('https://tgc.tmall.com/api/v1/orderNew/createPanamaOrderExportTask.htm', params=params, headers=headers)
        if not resp.ok:
            raise Exception('req create order export task status code: {}'.format(resp.status_code))
        resp_json = resp.json()
        if resp_json['success']:
            print('create order export task data: {}'.format(resp_json['data']))
            return int(resp_json['data'])
        raise Exception('resp fail: {}'.format(json.dumps(resp_json)))
    
    def taskRecord(self, task_id: int) -> str: # 根据任务id查找任务结果下载链接（最近100条），任务还在执行返回None
        params = {
            'pageNo': 1,
            'pageSize': 100
        }
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cookie': self._cookie,
            'priority': 'u=1, i',
            'referer': 'https://tgc.tmall.com/ds/page/supplier/order-download?c2mNavigatorShellPage=1&c2mNavigatorPageOpener=0',
            'user-agent': self._user_agent,
        }
        resp = requests.get('https://tgc.tmall.com/api/v1/orderNew/taskRecordList.htm', params=params, headers=headers)
        if not resp.ok:
            raise Exception('req task record status code: {}'.format(resp.status_code))
        resp_json = resp.json()
        for record in resp_json['data']['data']:
            if int(record['id']) != task_id:
                continue
            if record['status'] == '导出处理中':
                return None # 告诉还在处理
            if record['status'] == '导出成功' and int(record['percent']) == 100 and int(record['errorCount']) == 0:
                print('task record: {}, {}'.format(task_id, record['fileDownList'][0]['downUrl']))
                return record['fileDownList'][0]['downUrl']
        raise Exception('req task record not found: {}'.format(task_id))

    def download(self, url: str, filename: str) -> str:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'cookie': self._cookie,
            'priority': 'u=1, i',
            'upgrade-insecure-requests': '1',
            'user-agent': self._user_agent,
        }
        resp = requests.get(url=url, headers=headers)
        if not resp.ok:
            raise Exception('download status code: {}'.format(resp.status_code))
        filepath = 'output/{}.xlsx'.format(filename)
        with open(filepath, 'wb') as f:
            f.write(resp.content)
        return filepath