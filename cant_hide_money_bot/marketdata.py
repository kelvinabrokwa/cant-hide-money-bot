"""
This module contains functions for getting stock prices
"""

import logging

import httpx
import requests

from .std import Mode, Symbol, SymbolData, TimedCache, TradeError, USD

# $1 always trades for $1
USD_SYMBOL_DATA = SymbolData(bid=1, ask=1, volume=9999999999999, currency='USD')

# In DEV mode we don't want to actually hit the market data API -- return this instead
DEV_SYMBOL_DATA = SymbolData(bid=99, ask=100, volume=1000000, currency='USD')


def error(symbol):
    return TradeError(
        f"Could not fetch market data for {symbol}. Are you sure that's a real ticker? Is the market open?")


def validate_symbol_data(symbol_data: SymbolData) -> SymbolData:
    if any(value is None for value in [symbol_data.bid, symbol_data.ask, symbol_data.volume, symbol_data.currency]):
        raise TradeError('It appears the market is currently not open. Try again at another time.')

    if symbol_data.bid == 0 or symbol_data.ask == 0:
        raise TradeError('Market data API return a price of $0. I cannot execute this trade right now.')

    return symbol_data


class MarketData:
    """
    This class caches market data for 5 minutes to reduce API calls
    Query's Alpha Advantage and Yahoo Finance
    """

    def __init__(self, rapid_api_key: str, mode: Mode, symbol_data_for_test=None) -> None:
        self.rapid_api_key = rapid_api_key
        self.mode = mode
        self.cache = TimedCache(max_age_seconds=300)
        self.symbol_data_for_test = symbol_data_for_test

    async def get_symbol_data(self, symbol: Symbol, use_cache: bool) -> SymbolData:
        if symbol == USD:
            return USD_SYMBOL_DATA

        if self.mode is Mode.DEV:
            if self.symbol_data_for_test is not None:
                symbol_data = self.symbol_data_for_test
            else:
                symbol_data = DEV_SYMBOL_DATA
        else:
            if ((symbol_data := self.cache.get(symbol)) is not None) and use_cache:
                logging.info(f'found {symbol} {symbol_data} in cache')
                return symbol_data

            symbol_data = await self.yahoo(symbol)

        self.cache.put(symbol, symbol_data)

        return symbol_data

    async def yahoo(self, symbol: Symbol) -> SymbolData:
        url = f'https://query1.finance.yahoo.com/v7/finance/quote'
        query_string = {
            'corsDomain': 'finance.yahoo.com',
            'symbols': symbol,
            'region': 'US'
        }
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=query_string)
        if response.status_code != requests.codes.ok or response.text == '':
            raise error(symbol)
        data = response.json().get('quoteResponse', {}).get('result', {})
        if len(data):
            data = data[0]
        else:
            data = {}

        bid = ask = data.get('regularMarketPrice')
        volume = data.get('regularMarketVolume')
        currency = data.get('currency')

        return validate_symbol_data(SymbolData(
            bid=bid,
            ask=ask,
            volume=volume,
            currency=currency))

    async def _yahoo(self, symbol: Symbol) -> SymbolData:
        """
        Yahoo Finance API
        """
        url = 'https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-summary'
        querystring = {'symbol': symbol, 'region': 'US'}
        headers = {
            'x-rapidapi-key': self.rapid_api_key,
            'x-rapidapi-host': 'apidojo-yahoo-finance-v1.p.rapidapi.com',
        }
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, params=querystring)
        if response.status_code != requests.codes.ok or response.text == '':
            raise error(symbol)

        summary = response.json().get('summaryDetail')
        if not summary or not all(key in summary for key in ['bid', 'ask', 'volume', 'currency']):
            raise error(symbol)

        bid = summary.get('bid', {}).get('raw')
        ask = summary.get('ask', {}).get('raw')
        volume = summary.get('volume', {}).get('raw')
        currency = summary.get('currency')

        if bid is None or ask is None:
            bid = ask = response.json().get('price', {}).get('regularMarketPrice', {}).get('raw')

        return validate_symbol_data(SymbolData(
            bid=bid,
            ask=ask,
            volume=volume,
            currency=currency))

    async def alpha_advantage(self, symbol: Symbol) -> SymbolData:
        """
        Alpha Advantage API
        """
        url = 'https://alpha-vantage.p.rapidapi.com/query'
        querystring = {'function': 'GLOBAL_QUOTE', 'symbol': symbol}
        headers = {
            'x-rapidapi-key': self.rapid_api_key,
            'x-rapidapi-host': 'alpha-vantage.p.rapidapi.com'
        }
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, params=querystring)
        if response.status_code != requests.codes.ok or response.text == '':
            raise error(symbol)

        try:
            quote = response.json().get('Global Quote')
        except ValueError:
            raise TradeError(f'Error fetching symbol data for {symbol}. Could not parse JSON.')

        if not quote or any(key not in quote for key in ['price', 'volume']):
            raise error(symbol)

        bid = ask = quote['01.price']
        volume = quote['06.volume']
        currency = 'USD'

        return validate_symbol_data(SymbolData(
            bid=bid,
            ask=ask,
            volume=volume,
            currency=currency))
