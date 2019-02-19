from tshistory.testutil import utcdt, genserie, assert_df


def test_base(client, engine, tsh):
    ts = client.get('no-such-series')
    assert ts is None

    assert not client.exists('no-such-series')

    meta = client.metadata('no-such-series')
    assert meta == {
        'message': '`no-such-series` does not exists'
    }

    series_in = genserie(utcdt(2018, 1, 1), 'H', 3)
    client.insert('test', series_in, 'Babar',
                  insertion_date=utcdt(2019, 1, 1))
    assert client.exists('test')

    # now let's get it back
    ts = client.get('test')
    assert_df("""
2018-01-01 00:00:00+00:00    0.0
2018-01-01 01:00:00+00:00    1.0
2018-01-01 02:00:00+00:00    2.0
""", ts)

    meta = client.metadata('test', internal=True)
    assert meta == {
        'tzaware': True,
        'index_type': 'datetime64[ns, UTC]',
        'value_type': 'float64',
        'index_dtype': '|M8[ns]',
        'index_names': [],
        'value_dtype': '<f8'
    }

    # update
    client.metadata('test', update={'desc': 'banana spot price'})

    meta = client.metadata('test', internal=False)
    assert meta == {
        'desc': 'banana spot price',
    }

    # check the insertion_date
    series_in = genserie(utcdt(2018, 1, 2), 'H', 3)
    client.insert('test', series_in, 'Babar')

    d1, d2 = tsh.insertion_dates(engine, 'test')
    assert d1 == utcdt(2019, 1, 1)
    assert d2 > d1
