import json
import threading

import requests
import pandas as pd

from tshistory.util import fromjson, tojson, tzaware_serie
from tshistory.testutil import utcdt


class Client:
    baseuri = None
    tzcache = None
    _lock = threading.Lock()

    def __init__(self, uri):
        self.baseuri = uri
        self.tzcache = {}

    def exists(self, name):
        meta = self.metadata(name)
        if 'message' in meta and meta['message'].endswith('does not exists'):
            return False
        return True

    def insert(self, name, series, author, insertion_date=None):
        res = requests.patch(f'{self.baseuri}/series/state', data={
            'name': name,
            'author': author,
            'series': tojson(series),
            'insertion_date': insertion_date.isoformat() if insertion_date else None,
            'tzaware': tzaware_serie(series)
        })
        assert res.status_code in (200, 201)

    def metadata(self, name, update=None, internal=False):
        if update is not None:
            assert isinstance(update, dict)
            res = requests.put(f'{self.baseuri}/series/metadata', data={
                'name': name,
                'metadata': json.dumps(update)
            })
            return res.status_code

        res = requests.get(f'{self.baseuri}/series/metadata', params={
            'name': name,
            'all': int(internal)
        })
        return res.json()

    def get(self, name):
        res = requests.get(f'{self.baseuri}/series/state', params={
            'name': name
        })
        if res.status_code == 404:
            return None
        assert res.status_code == 200

        with self._lock:
            if name not in self.tzcache:
                self.tzcache[name] = self.metadata(name, internal=True)['tzaware']
            tzinfo = self.tzcache[name]
        return fromjson(res.text, name, tzinfo)
