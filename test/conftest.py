import io
from pathlib import Path
from functools import partial

import webtest
import pytest
from pytest_sa_pg import db
import responses
from sqlalchemy import create_engine, MetaData

from tshistory import schema
from tshistory_rest import app
from tshistory_client import api

DATADIR = Path(__file__).parent / 'data'


@pytest.fixture(scope='session')
def engine(request):
    db.setup_local_pg_cluster(request, DATADIR, 5433, {
        'timezone': 'UTC',
        'log_timezone': 'UTC'}
    )
    e = create_engine('postgresql://localhost:5433/postgres')
    schema.reset(e)
    schema.init(e, MetaData())
    return e


class WebTester(webtest.TestApp):

    def _check_status(self, status, res):
        try:
            super(WebTester, self)._check_status(status, res)
        except:
            print(res.errors)
            # raise <- default behaviour on 4xx is silly

    def _gen_request(self, method, url, params,
                     headers=None,
                     extra_environ=None,
                     status=None,
                     upload_files=None,
                     expect_errors=False,
                     content_type=None):
        """
        Do a generic request.
        PATCH: *bypass* all transformation as params comes
               straight from a prepared (python-requests) request.
        """
        environ = self._make_environ(extra_environ)

        environ['REQUEST_METHOD'] = str(method)
        url = str(url)
        url = self._remove_fragment(url)
        req = self.RequestClass.blank(url, environ)

        req.environ['wsgi.input'] = io.BytesIO(params.encode('utf-8'))
        req.content_length = len(params)
        if headers:
            req.headers.update(headers)
        return self.do_request(req, status=status,
                               expect_errors=expect_errors)


def get_request_bridge(client, request):
    resp = client.get(request.url,
                      params=request.body,
                      headers=request.headers)
    return (resp.status_code, resp.headers, resp.body)


def patch_request_bridge(method):
    def bridge(request):
        resp = method(request.url,
                      params=request.body,
                      headers=request.headers)
        return (resp.status_code, resp.headers, resp.body)
    return bridge


URI = 'http://test-uri'

@pytest.fixture(scope='session')
def client(engine):
    wsgitester = WebTester(app.make_app(engine))
    with responses.RequestsMock(assert_all_requests_are_fired=False) as resp:
        resp.add_callback(
            responses.GET, 'http://test-uri/series/state',
            callback=partial(get_request_bridge, wsgitester)
        )

        resp.add_callback(
            responses.PATCH, 'http://test-uri/series/state',
            callback=patch_request_bridge(wsgitester.patch)
        )

        yield api.Client(URI)
