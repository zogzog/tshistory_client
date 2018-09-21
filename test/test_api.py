from tshistory.testutil import utcdt, genserie, assert_df


def test_base(client):
    ts = client.get('no-such-series')
    assert ts is None

    series_in = genserie(utcdt(2018, 1, 1), 'H', 3)
    client.insert('test', series_in, 'Babar')

    # now let's get it back
    ts = client.get('test')
    assert_df("""
2018-01-01 00:00:00    0.0
2018-01-01 01:00:00    1.0
2018-01-01 02:00:00    2.0
""", ts)
