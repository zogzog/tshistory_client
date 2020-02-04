TSHISTORY CLIENT
================

This library provides easy access to [tshistory][tshistory] instances exposed with
the help of [tshistory_rest][tshistory_rest] (http end points).

All the base `tshistory` api is available from there.


# Examples

## Direct invocation

```python
 from tshistory_client.api import Client
 
 c = Client('http://my.tshistory.instance/api')
 series = c.get('banana_spot_price')
```

## Using the tshistory api

```python
 from tshistory.api import timeseries
 
 c = timeseries('http://my.tshistory.instance/api')
 series = c.get('banana_spot_price')
```

Of course for this to work you need to have tshistory locally
installed (through e.g. `pip install tshistory`).


[tshistory_rest]: https://bitbucket.org/pythonian/tshistory_rest
[tshistory]: https://bitbucket.org/pythonian/tshistory
