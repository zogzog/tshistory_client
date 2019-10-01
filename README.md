TSHISTORY CLIENT
================

This library provides easy access to tshistory instances exposed with
the help of [tshistory_rest][tshistory_rest] (http end points).


# Examples

```python
 from tshistory_client.api import Client
 
 c = Client('http://my.saturn.instance/api')
 series = c.get('banana_spot_price')
```

Most of the base `tshistory` api is available from there.


[tshistory_rest]: https://bitbucket.org/pythonian/tshistory_rest
