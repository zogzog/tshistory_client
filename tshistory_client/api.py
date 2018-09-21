import requests
import pandas as pd

from tshistory.util import fromjson


class Client:

    def __init__(self, uri):
        self.baseuri = uri

    def get(self, name):
        res = requests.get(f'{self.baseuri}/series/state', params={
            'name': name
        })
        if res.status_code == 404:
            return None
        assert False
