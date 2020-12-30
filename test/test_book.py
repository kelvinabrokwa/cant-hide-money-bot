from datetime import datetime

import pandas
import pytest

from cant_hide_money_bot.book import TRADER_INIT_USD, all_portfolios
from cant_hide_money_bot.marketdata import MarketData
from cant_hide_money_bot.std import Mode, SymbolData


@pytest.mark.asyncio
async def test_price_is_the_same():
    trader = 'kelvin'

    def create_trade(symbol, dir_, qty, price):
        return {
            'symbol': symbol,
            'dir': dir_,
            'qty': qty,
            'price': price,
            'time': datetime.fromisoformat('2020-01-01T00:09:30'),
            'trader': trader,
            'guild_id': 100,
        }

    def create_portfolio(symbol, shares, value, position):
        return {'symbol': symbol, 'shares': shares, 'value': value, 'position': position}

    market_data = MarketData('', Mode.DEV)

    def symbol_data(current_price):
        return SymbolData(bid=current_price, ask=current_price, volume=1000000, currency='USD')

    #
    # Single trade
    #

    async def t(current_price, expected_portfolio_value):
        book = pandas.DataFrame([
            create_trade('ZVZZT', 'BUY', 100, 100)
        ])
        market_data.symbol_data_for_test = symbol_data(current_price)
        portfolios = await all_portfolios(book, market_data)
        actual_portfolio = portfolios[trader]
        expected_portfolio = pandas.DataFrame([
            create_portfolio('Portfolio', None, expected_portfolio_value, None),
        ])
        assert actual_portfolio.loc[actual_portfolio['symbol'] == 'Portfolio']['value'][0] == \
               expected_portfolio.loc[expected_portfolio['symbol'] == 'Portfolio']['value'][0]

    await t(100, TRADER_INIT_USD)
    # If price goes down $1, you lose $100
    await t(99, TRADER_INIT_USD - 100)
    # If price goes up $1, you make $100
    await t(101, TRADER_INIT_USD + 100)

    #
    # Multiple trades
    #

    async def t(current_price, expected_portfolio_value):
        book = pandas.DataFrame([
            create_trade('ZVZZT', 'BUY', 100, 100),
            create_trade('ZVZZT', 'BUY', 100, 101)
        ])
        market_data.symbol_data_for_test = symbol_data(current_price)
        portfolios = await all_portfolios(book, market_data)
        actual_portfolio = portfolios[trader]
        expected_portfolio = pandas.DataFrame([
            create_portfolio('Portfolio', None, expected_portfolio_value, None),
        ])
        assert actual_portfolio.loc[actual_portfolio['symbol'] == 'Portfolio']['value'][0] == \
               expected_portfolio.loc[expected_portfolio['symbol'] == 'Portfolio']['value'][0]

    await t(101, TRADER_INIT_USD + 100)
    await t(100, TRADER_INIT_USD - 100)
    await t(99, TRADER_INIT_USD - 300)
    await t(102, TRADER_INIT_USD + 300)

    #
    # Buys and sells
    #

    async def t(current_price, expected_portfolio_value):
        book = pandas.DataFrame([
            create_trade('ZVZZT', 'BUY', 100, 100),
            create_trade('ZVZZT', 'SELL', 100, 101),
            create_trade('ZVZZT', 'BUY', 100, 102),
        ])
        market_data.symbol_data_for_test = symbol_data(current_price)
        portfolios = await all_portfolios(book, market_data)
        actual_portfolio = portfolios[trader]
        expected_portfolio = pandas.DataFrame([
            create_portfolio('Portfolio', None, expected_portfolio_value, None),
        ])
        assert actual_portfolio.loc[actual_portfolio['symbol'] == 'Portfolio']['value'][0] == \
               expected_portfolio.loc[expected_portfolio['symbol'] == 'Portfolio']['value'][0]

    await t(100, TRADER_INIT_USD - 100)
    await t(101, TRADER_INIT_USD)
