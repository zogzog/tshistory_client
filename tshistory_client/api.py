import json
import zlib

import requests
import pandas as pd
import pytz

from tshistory.util import (
    nary_unpack,
    fromjson,
    numpy_deserialize,
    tojson,
    tzaware_serie,
    unpack_history
)
from tshistory.testutil import utcdt


def strft(dt):
    """Format dt object into str.

    We first make sure dt is localized (aka non-naive). If dt is naive
    UTC is automatically added as tzinfo.
    """
    is_naive = dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None
    if is_naive:
        dt = pytz.UTC.localize(dt)
    return dt.isoformat()


def decodeseries(bytestream):
    bmeta, bindex, bvalues = nary_unpack(
        zlib.decompress(bytestream)
    )
    meta = json.loads(bmeta)
    index, values = numpy_deserialize(bindex, bvalues, meta)
    series = pd.Series(values, index=index)
    if meta['tzaware']:
        series = series.tz_localize('UTC')
    return series


class Client:
    baseuri = None
    tzcache = None

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

    def get(self, name,
            revision_date=None,
            from_value_date=None,
            to_value_date=None):
        args = {
            'name': name,
            'mode': 'numpy'
        }
        if revision_date:
            args['insertion_date'] = strft(revision_date)
        if from_value_date:
            args['from_value_date'] = strft(from_value_date)
        if to_value_date:
            args['to_value_date'] = strft(to_value_date)
        res = requests.get(
            f'{self.baseuri}/series/state', params=args
        )
        if res.status_code == 404:
            return None
        res.raise_for_status()
        assert res.status_code == 200

        return decodeseries(res.content)

    def staircase(self, name, delta,
            from_value_date=None,
            to_value_date=None):
        args = {
            'name': name,
            'delta': delta,
            'mode': 'numpy'
        }
        if from_value_date:
            args['from_value_date'] = strft(from_value_date)
        if to_value_date:
            args['to_value_date'] = strft(to_value_date)
        res = requests.get(
            f'{self.baseuri}/series/staircase', params=args
        )
        if res.status_code == 404:
            return None
        res.raise_for_status()
        assert res.status_code == 200

        return decodeseries(res.content)

    def history(self, name,
                from_insertion_date=None,
                to_insertion_date=None,
                from_value_date=None,
                to_value_date=None):
        args = {
            'name': name,
            'mode': 'numpy'
        }
        if from_insertion_date:
            args['from_insertion_date'] = strft(from_insertion_date)
        if to_insertion_date:
            args['to_insertion_date'] = strft(to_insertion_date)
        if from_value_date:
            args['from_value_date'] = strft(from_value_date)
        if to_value_date:
            args['to_value_date'] = strft(to_value_date)
        res = requests.get(
            f'{self.baseuri}/series/history', params=args
        )
        if res.status_code == 404:
            return None
        res.raise_for_status()
        assert res.status_code == 200

        meta, hist = unpack_history(res.content)
        return hist

    def list_series(self):
        res = requests.get(f'{self.baseuri}/series/catalog')
        assert res.status_code == 200

        return res.json()
