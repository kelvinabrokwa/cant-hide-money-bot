from cant_hide_money_bot.server import parse_qty
from cant_hide_money_bot.std import Dollars, Shares


def test_parse_qty():
    usd_100 = parse_qty('$100')
    assert type(usd_100) == Dollars
    assert float(usd_100) == 100.

    shares_200 = parse_qty('200')
    assert type(shares_200) == Shares
    assert float(shares_200) == 200.
