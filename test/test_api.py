import pandas as pd
import pytest

from tshistory import tsio
from tshistory.testutil import (
    assert_df,
    assert_hist,
    genserie,
    utcdt
)


def test_naive(client, engine, tsh):
    series_in = genserie(pd.Timestamp('2018-1-1'), 'H', 3)
    client.update('test-naive', series_in, 'Babar',
                  insertion_date=utcdt(2019, 1, 1))

    # now let's get it back
    ts = client.get('test-naive')
    assert_df("""
2018-01-01 00:00:00    0.0
2018-01-01 01:00:00    1.0
2018-01-01 02:00:00    2.0
""", ts)
    assert not getattr(ts.index.dtype, 'tz', False)


def test_base(client, engine, tsh):
    assert repr(client) == "tshistory-http-client(uri='http://test-uri')"

    ts = client.get('no-such-series')
    assert ts is None

    assert not client.exists('no-such-series')

    meta = client.metadata('no-such-series')
    assert meta == {
        'message': '`no-such-series` does not exists'
    }

    series_in = genserie(utcdt(2018, 1, 1), 'H', 3)
    client.update('test', series_in, 'Babar',
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

    # out of range
    ts = client.get(
        'test',
        from_value_date=utcdt(2020, 1, 1, 2),
        to_value_date=utcdt(2020, 1, 1, 2)
    )
    assert len(ts) == 0
    assert ts.name == 'test'

    meta = client.metadata('test', all=True)
    assert meta == {
        'tzaware': True,
        'index_type': 'datetime64[ns, UTC]',
        'value_type': 'float64',
        'index_dtype': '|M8[ns]',
        'value_dtype': '<f8'
    }

    # update
    client.update_metadata('test', {'desc': 'banana spot price'})

    meta = client.metadata('test', all=False)
    assert meta == {
        'desc': 'banana spot price',
    }

    # check the insertion_date
    series_in = genserie(utcdt(2018, 1, 2), 'H', 3)
    client.update('test', series_in, 'Babar',
                  metadata={'event': 'hello'},
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

    client.update('test2', series_in, 'Babar')
    series = client.catalog()
    assert ['test', 'primary'] in series[('db://localhost:5433/postgres', 'tsh')]
    assert ['test2', 'primary'] in series[('db://localhost:5433/postgres', 'tsh')]

    client.replace('test2', genserie(utcdt(2020, 1, 1), 'D', 3), 'Babar')
    series = client.get('test2')
    assert_df("""
2020-01-01 00:00:00+00:00    0.0
2020-01-02 00:00:00+00:00    1.0
2020-01-03 00:00:00+00:00    2.0
""", series)

    type = client.type('test2')
    assert type == 'primary'

    ival = client.interval('test2')
    assert ival.left == pd.Timestamp('2020-01-01 00:00:00+0000', tz='UTC')
    assert ival.right == pd.Timestamp('2020-01-03 00:00:00+0000', tz='UTC')

    client.rename('test2', 'test3')
    assert not client.exists('test2')
    assert client.exists('test3')

    client.delete('test3')
    assert not client.exists('test3')


def test_staircase_history(client, tsh):
    # each days we insert 7 data points
    for idx, idate in enumerate(pd.date_range(start=utcdt(2015, 1, 1),
                                              end=utcdt(2015, 1, 4),
                                              freq='D')):
        series = genserie(start=idate, freq='H', repeat=7)
        client.update(
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
    assert series.name == 'staircase'

    assert_df("""
2015-01-01 04:00:00+00:00    4.0
2015-01-01 05:00:00+00:00    5.0
2015-01-01 06:00:00+00:00    6.0
2015-01-02 03:00:00+00:00    3.0
2015-01-02 04:00:00+00:00    4.0
2015-01-02 05:00:00+00:00    5.0
""", series)

    hist = client.history('staircase')
    assert len(hist) == 4
    hist = client.history(
        'staircase',
        from_insertion_date=utcdt(2015, 1, 2),
        to_insertion_date=utcdt(2015, 1, 3)
    )
    assert len(hist) == 2
    hist = client.history(
        'staircase',
        from_value_date=utcdt(2015, 1, 1, 3),
        to_value_date=utcdt(2015, 1, 2, 1)
    )

    assert all(
        series.name == 'staircase'
        for series in hist.values()
    )

    assert_hist("""
insertion_date             value_date               
2015-01-01 00:00:00+00:00  2015-01-01 03:00:00+00:00    3.0
                           2015-01-01 04:00:00+00:00    4.0
                           2015-01-01 05:00:00+00:00    5.0
                           2015-01-01 06:00:00+00:00    6.0
2015-01-02 00:00:00+00:00  2015-01-01 03:00:00+00:00    3.0
                           2015-01-01 04:00:00+00:00    4.0
                           2015-01-01 05:00:00+00:00    5.0
                           2015-01-01 06:00:00+00:00    6.0
                           2015-01-02 00:00:00+00:00    0.0
                           2015-01-02 01:00:00+00:00    1.0
""", hist)


def test_staircase_history_naive(client, tsh):
    # each days we insert 7 data points
    from datetime import datetime
    for idx, idate in enumerate(pd.date_range(start=utcdt(2015, 1, 1),
                                              end=utcdt(2015, 1, 4),
                                              freq='D')):
        series = genserie(
            start=idate.tz_convert(None),
            freq='H',
            repeat=7
        )
        client.update(
            'staircase-naive',
            series, 'Babar',
            insertion_date=idate
        )

    series = client.staircase(
        'staircase-naive',
        pd.Timedelta(hours=3),
        from_value_date=datetime(2015, 1, 1, 4),
        to_value_date=datetime(2015, 1, 2, 5)
    )
    assert series.name == 'staircase-naive'

    assert_df("""
2015-01-01 04:00:00    4.0
2015-01-01 05:00:00    5.0
2015-01-01 06:00:00    6.0
2015-01-02 03:00:00    3.0
2015-01-02 04:00:00    4.0
2015-01-02 05:00:00    5.0
""", series)

    # series = client.staircase(
    #     'staircase-naive',
    #     pd.Timedelta(hours=3),
    #     from_value_date=datetime(2015, 1, 1, 4),
    #     to_value_date=datetime(2015, 1, 2, 5)
    # )

    hist = client.history('staircase-naive')
    assert len(hist) == 4
    hist = client.history(
        'staircase-naive',
        from_insertion_date=datetime(2015, 1, 2),
        to_insertion_date=datetime(2015, 1, 3)
    )
    assert len(hist) == 2
    hist = client.history(
        'staircase-naive',
        from_value_date=datetime(2015, 1, 1, 3),
        to_value_date=datetime(2015, 1, 2, 1)
    )

    assert all(
        series.name == 'staircase-naive'
        for series in hist.values()
    )

    assert_hist("""
insertion_date             value_date         
2015-01-01 00:00:00+00:00  2015-01-01 03:00:00    3.0
                           2015-01-01 04:00:00    4.0
                           2015-01-01 05:00:00    5.0
                           2015-01-01 06:00:00    6.0
2015-01-02 00:00:00+00:00  2015-01-01 03:00:00    3.0
                           2015-01-01 04:00:00    4.0
                           2015-01-01 05:00:00    5.0
                           2015-01-01 06:00:00    6.0
                           2015-01-02 00:00:00    0.0
                           2015-01-02 01:00:00    1.0
""", hist)


def test_formula(client, engine, tsh):
    tsh.update(
        engine,
        pd.Series(
            [1, 2, 3],
            index=pd.date_range(
                pd.Timestamp('2020-1-1', tz='UTC'),
                freq='D',
                periods=3
            )
        ),
        'in-a-formula',
        'Babar'
    )

    with pytest.raises(SyntaxError):
        client.register_formula(
            'new-formula',
            '(+ 3'
        )

    with pytest.raises(ValueError):
        client.register_formula(
            'new-formula',
            '(+ 3 (series "lol"))'
        )

    client.register_formula(
        'new-formula',
        '(+ 3 (series "lol"))',
        reject_unknown=False
    )


    with pytest.raises(AssertionError):
        client.register_formula(
            'new-formula',
            '(+ 3 (series "in-a-formula"))',
        )


    client.register_formula(
        'new-formula',
        '(+ 3 (series "in-a-formula"))',
        update=True
    )

    series = client.get('new-formula')
    assert_df("""
2020-01-01 00:00:00+00:00    4.0
2020-01-02 00:00:00+00:00    5.0
2020-01-03 00:00:00+00:00    6.0
""", series)

    assert client.formula('new-formula') == '(+ 3 (series "in-a-formula"))'
    assert client.formula('lol') is None


def test_multisources(client, engine):
    series = genserie(utcdt(2020, 1, 1), 'D', 3)
    tsh = tsio.timeseries('other')

    tsh.update(engine, series, 'test-other', 'Babar')

    client.update('test-mainsource', series, 'Babar')
    with pytest.raises(ValueError) as err:
        client.update('test-other', series, 'Babar')
    assert err.value.args[0] == 'not allowed to update to a secondary source'
    with pytest.raises(ValueError) as err:
        client.replace('test-other', series, 'Babar')
    assert err.value.args[0] == 'not allowed to replace to a secondary source'

    cat = client.catalog()
    assert cat == {
        ('db://localhost:5433/postgres', 'other'): [
            ['test-other', 'primary']
        ],
        ('db://localhost:5433/postgres', 'tsh'): [
            ['test-naive', 'primary'],
            ['test', 'primary'],
            ['staircase', 'primary'],
            ['staircase-naive', 'primary'],
            ['in-a-formula', 'primary'],
            ['test-mainsource', 'primary'],
            ['new-formula', 'formula']
        ]
    }
    cat = client.catalog(allsources=False)
    assert ('db://localhost:5433/postgres', 'tsh') in cat
    assert ('db://localhost:5433/postgres', 'other') not in cat
