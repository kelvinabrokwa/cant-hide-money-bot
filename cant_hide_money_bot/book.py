import asyncio
from typing import Dict, List

import numpy
import pandas

from . import std, marketdata

# Disable the following pandas warning:
# A value is trying to be set on a copy of a slice from a DataFrame.
pandas.options.mode.chained_assignment = None
pandas.options.display.float_format = '{:,}'.format

# BookDataFrame column names
AVG_COST = 'avg cost'
CURRENT_PRICE = 'current price'
DIR = 'dir'
DOLLARS = 'dollars'
GUILD_ID = 'guild_id'
MARK_PNL = 'mark pnl'
MULT = 'mult'
PORTFOLIO = 'Portfolio'
POSITION = 'position'
QTY = 'qty'
RETURN = 'return'
SHARES = 'shares'
SYMBOL = 'symbol'
TIME = 'time'
TRADE_PRICE = 'price'
TRADER = 'trader'
VALUE = 'value'
USD = 'usd'
USD_SYMBOL = 'USD'

FUND_INIT_USD = 0
TRADER_INIT_USD = 1000000

BUY = 'BUY'


def filter_book_for_guild_id(book: pandas.DataFrame, guild_id: int) -> pandas.DataFrame:
    return book[book[GUILD_ID] == guild_id]


def filter_book_for_trader(book: pandas.DataFrame, trader: std.Trader) -> pandas.DataFrame:
    return book[book[TRADER] == trader]


def shares_and_dollars(book: pandas.DataFrame) -> pandas.DataFrame:
    book[MULT] = book[DIR].apply(lambda dir_: 1 if dir_ == BUY else -1)
    book[SHARES] = book[QTY] * book[MULT]
    # You get negative dollars when you go long and positive dollars when you sell short
    book[DOLLARS] = book[QTY] * book[TRADE_PRICE] * (-1 * book[MULT])
    return book


def position_for_symbol(book: pandas.DataFrame, trader: std.Trader, symbol: std.Symbol) -> int:
    book = shares_and_dollars(book)
    book = book[(book[TRADER] == trader) & (book[SYMBOL] == symbol)]
    return book[SHARES].sum()


def usd_for_trader(book: pandas.DataFrame, trader: std.Trader) -> float:
    book = shares_and_dollars(book)
    book = filter_book_for_trader(book, trader)
    return book[DOLLARS].sum() + TRADER_INIT_USD


def compute_current_value(book_with_shares_and_dollars: pandas.DataFrame, current_prices: pandas.DataFrame,
                          usd_init: float) -> pandas.DataFrame:
    # Group by symbol and sum up shares, qty, and dollars
    book = book_with_shares_and_dollars.groupby(SYMBOL, as_index=False).agg({SHARES: 'sum', DOLLARS: 'sum'})
    # Compute average cost
    book[AVG_COST] = book[DOLLARS].apply(lambda dollars: dollars * -1) / book[SHARES]
    # Join on prices tables
    book = book.merge(current_prices, on=SYMBOL, how='left')
    # When the position for a symbol is 0, we will not query for the price so book[CURRENT_PRICE] will be
    # NaN -- make these 0
    book[CURRENT_PRICE] = book[CURRENT_PRICE].fillna(value=0)
    # Calculate current value for positions
    book[VALUE] = book[SHARES] * book[CURRENT_PRICE]
    # Calculate the dollar value of all our positions and our cash
    book[USD] = book[DOLLARS] + book[VALUE]
    # Calculate number of uninvested dollars
    usd = book[DOLLARS].sum() + usd_init
    # Calculate the value of the portfolio
    value = book[USD].sum() + usd_init
    # Calculate mark pnl
    book[MARK_PNL] = (book[CURRENT_PRICE] - book[AVG_COST]) * book[SHARES]
    # Calculate return
    book[MULT] = book[SHARES].apply(lambda pos: 1 if pos >= 0 else -1)
    book[RETURN] = ((book[CURRENT_PRICE] - book[AVG_COST]) / book[AVG_COST]) * book[MULT] * 100.
    # Determine whether we are LONG or SHORT by position
    book[POSITION] = book[SHARES].apply(lambda pos: 'LONG' if pos >= 0 else 'SHORT')
    # Filter out symbols with position = 0
    book = book[book[SHARES] != 0]
    # Select the columns we want to display
    book = book[[SYMBOL, SHARES, POSITION, VALUE, AVG_COST, CURRENT_PRICE, MARK_PNL, RETURN]]
    # Add the USD and Portfolio columns
    book = book.append([
        {
            SYMBOL: USD_SYMBOL,
            SHARES: numpy.nan,
            VALUE: usd,
            AVG_COST: numpy.nan,
            CURRENT_PRICE: numpy.nan,
            MARK_PNL: numpy.nan,
            RETURN: numpy.nan,
            POSITION: '',
        },
        {
            SYMBOL: PORTFOLIO,
            SHARES: numpy.nan,
            VALUE: value,
            AVG_COST: numpy.nan,
            CURRENT_PRICE: numpy.nan,
            MARK_PNL: numpy.nan,
            RETURN: numpy.nan,
            POSITION: '',
        },
    ])

    return book


def get_all_symbols_with_non_zero_position(book_with_shares: pandas.DataFrame) -> List[str]:
    book = book_with_shares.groupby([TRADER, SYMBOL], as_index=False).agg({SHARES: 'sum'})
    book = book[book[SHARES] != 0]
    return book[SYMBOL].unique()


async def get_current_prices(symbols, market_data: marketdata.MarketData) -> pandas.DataFrame:
    df = pandas.DataFrame(data=symbols, columns=[SYMBOL])
    df[CURRENT_PRICE] = None

    symbols_data = await market_data.get_symbols_data(symbols, use_cache=True)

    for symbol, symbol_data in symbols_data.items():
        df.loc[df[SYMBOL] == symbol, CURRENT_PRICE] = symbol_data.mid()

    return df


async def all_portfolios(book: pandas.DataFrame, market_data: marketdata.MarketData) -> Dict[str, pandas.DataFrame]:
    book_with_shares_and_dollars = shares_and_dollars(book)
    all_symbols = get_all_symbols_with_non_zero_position(book_with_shares_and_dollars)
    current_prices = await get_current_prices(all_symbols, market_data)

    guild_portfolio = compute_current_value(book_with_shares_and_dollars, current_prices, FUND_INIT_USD)

    if not len(guild_portfolio.index):
        return {}

    portfolios = {'fund': guild_portfolio}

    for trader, trader_book in book.groupby(TRADER):
        if len(trader_book.index) > 0:
            portfolios[trader] = compute_current_value(trader_book, current_prices, TRADER_INIT_USD)

    return portfolios
