
def test_base(client):
    ts = client.get('no-such-series')
    assert ts is None

