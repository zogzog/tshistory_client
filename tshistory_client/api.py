import json
import requests
import pandas as pd

from tshistory.util import fromjson, tojson
from tshistory.testutil import utcdt


class Client:

    def __init__(self, uri):
        self.baseuri = uri

    def insert(self, name, series, author):
        res = requests.patch(f'{self.baseuri}/series/state', data={
            'name': name,
            'author': author,
            'series': tojson(series),
            'insertion_date': utcdt(2018, 1, 1).isoformat()
        })
        assert res.status_code in (200, 201)

    def get(self, name):
        res = requests.get(f'{self.baseuri}/series/state', params={
            'name': name
        })
        if res.status_code == 404:
            return None
        assert res.status_code == 200
        return fromjson(res.text, name)
