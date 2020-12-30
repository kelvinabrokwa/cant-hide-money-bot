"""
This module contains functions for getting stock prices
"""

import logging
import typing

import httpx
import requests

from . import std

# $1 always trades for $1
USD_SYMBOL_DATA = std.SymbolData(bid=1, ask=1, volume=9999999999999, currency='USD')

# In DEV mode we don't want to actually hit the market data API -- return this instead
DEV_SYMBOL_DATA = std.SymbolData(bid=99, ask=100, volume=1000000, currency='USD')


def error(symbols):
    return std.TradeError(
        f"Could not fetch market data for {', '.join(symbols)}. Are you sure these are real tickers? Is the market open?")


def validate_symbol_data(symbol_data: std.SymbolData, api_data: typing.Dict) -> std.SymbolData:
    if any(value is None for value in [symbol_data.bid, symbol_data.ask, symbol_data.volume, symbol_data.currency]):
        raise std.TradeError(f'It appears the market is currently not open -- try again at another time: {api_data}')

    if symbol_data.bid == 0 or symbol_data.ask == 0:
        raise std.TradeError(
            f'Market data API return a price of $0 -- I cannot execute this trade right now: {api_data}')

    return symbol_data


async def yahoo(symbols: typing.List[std.Symbol]) -> std.SymbolData:
    url = f'https://query1.finance.yahoo.com/v7/finance/quote'
    query_string = {
        'corsDomain': 'finance.yahoo.com',
        'symbols': ','.join(symbols),
        'region': 'US'
    }
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=query_string)
    if response.status_code != requests.codes.ok or response.text == '':
        raise error(symbols)
    api_data = response.json().get('quoteResponse', {}).get('result', [])

    def parse(api_data) -> typing.Tuple[std.Symbol, std.SymbolData]:
        bid = ask = api_data.get('regularMarketPrice')
        volume = api_data.get('regularMarketVolume')
        currency = api_data.get('currency')
        symbol = std.Symbol(api_data.get('symbol'))

        symbol_data = validate_symbol_data(std.SymbolData(
            bid=bid,
            ask=ask,
            volume=volume,
            currency=currency), api_data)

        return symbol, symbol_data

    symbols_and_data = [parse(d) for d in api_data]

    return {symbol: symbol_data for symbol, symbol_data in symbols_and_data}


class MarketData:
    """
    This class caches market data for 5 minutes to reduce API calls
    """

    def __init__(self, mode: std.Mode, symbol_data_for_test=None) -> None:
        self.mode = mode
        self.cache = std.TimedCache(max_age_seconds=300)
        self.symbol_data_for_test = symbol_data_for_test

    async def get_symbols_data(self, symbols: typing.List[std.Symbol], use_cache: bool) -> std.SymbolData:
        symbols_to_fetch = symbols
        results = {}

        if std.USD in symbols:
            results[std.USD] = USD_SYMBOL_DATA
            symbols_to_fetch = [symbol for symbol in symbols_to_fetch if symbol != std.USD]

        if self.mode is std.Mode.DEV:
            if self.symbol_data_for_test is not None:
                results.update({symbol: self.symbol_data_for_test for symbol in symbols_to_fetch})
            else:
                symbol_data = DEV_SYMBOL_DATA
        else:
            for symbol in symbols_to_fetch:
                if ((symbol_data := self.cache.get(symbol)) is not None) and use_cache:
                    logging.info(f'found {symbol} {symbol_data} in cache')
                    results[symbol] = symbol_data

            # Filter out the symbols we found in the cache
            symbols = [symbol for symbol in symbols_to_fetch if symbol not in results]

            symbols_and_data = await yahoo(symbols_to_fetch)

            results.update(symbols_and_data)

        for symbol, symbol_data in results.items():
            self.cache.put(symbol, symbol_data)

        return results
