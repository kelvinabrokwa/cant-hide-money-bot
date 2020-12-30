import pytest

from cant_hide_money_bot.marketdata import validate_symbol_data
from cant_hide_money_bot.std import SymbolData


def test_validate_symbol_data():
    with pytest.raises(Exception):
        validate_symbol_data(SymbolData(bid=None, ask=None, volume=None, currency=None))

    with pytest.raises(Exception):
        validate_symbol_data(SymbolData(bid=0, ask=10, volume=None, currency=None))

    with pytest.raises(Exception):
        validate_symbol_data(SymbolData(bid=10, ask=0, volume=None, currency=None))

    with pytest.raises(Exception):
        validate_symbol_data(SymbolData(bid=0, ask=0, volume=None, currency=None))