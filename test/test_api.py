import pandas as pd
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

    ts = client.get(
        'test',
        from_value_date=utcdt(2018, 1, 1, 2)
    )
    assert_df("""
2018-01-01 02:00:00+00:00    2.0
""", ts)

    ts = client.get(
        'test',
        to_value_date=utcdt(2018, 1, 1, 0)
    )
    assert_df("""
2018-01-01 00:00:00+00:00    0.0
""", ts)

    meta = client.metadata('test', internal=True)
    assert meta == {
        'tzaware': True,
        'index_type': 'datetime64[ns, UTC]',
        'value_type': 'float64',
        'index_dtype': '|M8[ns]',
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
    client.insert('test', series_in, 'Babar',
                  insertion_date=utcdt(2019, 1, 2))

    v1 = client.get('test', revision_date=utcdt(2019, 1, 1))
    assert_df("""
2018-01-01 00:00:00+00:00    0.0
2018-01-01 01:00:00+00:00    1.0
2018-01-01 02:00:00+00:00    2.0
""", v1)

    d1, d2 = tsh.insertion_dates(engine, 'test')
    assert d1 == utcdt(2019, 1, 1)
    assert d2 > d1

    client.insert('test2', series_in, 'Babar')
    series = client.list_series()
    assert series == {
        'test': 'primary',
        'test2': 'primary'
    }


def test_staircase(client, tsh):
    # each days we insert 7 data points
    for idx, idate in enumerate(pd.date_range(start=utcdt(2015, 1, 1),
                                              end=utcdt(2015, 1, 4),
                                              freq='D')):
        series = genserie(start=idate, freq='H', repeat=7)
        client.insert(
            'staircase',
            series, 'Babar',
            insertion_date=idate
        )

    series = client.staircase(
        'staircase',
        pd.Timedelta(hours=3),
        from_value_date=utcdt(2015, 1, 1, 4),
        to_value_date=utcdt(2015, 1, 2, 5)
    )

    assert_df("""
2015-01-01 04:00:00+00:00    4.0
2015-01-01 05:00:00+00:00    5.0
2015-01-01 06:00:00+00:00    6.0
2015-01-02 03:00:00+00:00    3.0
2015-01-02 04:00:00+00:00    4.0
2015-01-02 05:00:00+00:00    5.0
""", series)
