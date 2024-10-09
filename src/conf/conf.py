import json
import datetime
from typing import List, Tuple

class Conf(object):
    def __init__(self, date: str=None) -> None: # 日期格式2024-10-09
        with open('conf.json') as f:
            self._conf = json.load(f)
        self._conf['date'] = date if date is not None else datetime.date.today().strftime('%Y-%m-%d')
        print('conf date: {}'.format(self._conf['date']))
    
    def load(self, paths: List[str]) -> str:
        c = self._conf
        for p in paths:
            if c is None:
                raise Exception('paths: {} not found at: {}'.format(paths, p))
            c = c[p]
        if c is None:
            raise Exception('conf not found: {}'.format(paths))
        if type(c) is not str:
            raise Exception('conf not str: {}, {}'.format(paths, c))
        return c