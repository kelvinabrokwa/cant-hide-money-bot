import time

from cant_hide_money_bot.std import TimedCache


def test_timed_cache():
    cache = TimedCache(1)
    key = 'foo'
    value = 'bar'
    cache.put(key, value)
    assert cache.get(key) == value
    time.sleep(2)
    assert cache.get(key) is None
