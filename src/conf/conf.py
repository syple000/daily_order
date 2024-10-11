import json
from typing import List

class Conf(object):
    def __init__(self, confpath: str) -> None:
        self._confpath = confpath
        with open(confpath) as f:
            self._conf = json.load(f)
    
    def load(self, paths: List[str]) -> str:
        c = self._conf
        for p in paths:
            if c is None:
                raise Exception('{} paths: {} not found at: {}'.format(self._confpath, paths, p))
            c = c[p]
        if c is None:
            raise Exception('{} conf not found: {}'.format(self._confpath, paths))
        if type(c) is not str:
            raise Exception('{} conf not str: {}, {}'.format(self._confpath, paths, c))
        return c