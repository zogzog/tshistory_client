import json
import requests
import pandas as pd

from tshistory.util import fromjson, tojson, tzaware_serie
from tshistory.testutil import utcdt


class Client:
    baseuri = None
    tzcache = None

    def __init__(self, uri):
        self.baseuri = uri
        self.tzcache = {}

    def insert(self, name, series, author):
        res = requests.patch(f'{self.baseuri}/series/state', data={
            'name': name,
            'author': author,
            'series': tojson(series),
            'insertion_date': utcdt(2018, 1, 1).isoformat(),
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
        if name not in self.tzcache:
            self.tzcache[name] = self.metadata(name, internal=True)['tzaware']
        return fromjson(res.text, name, self.tzcache[name])
