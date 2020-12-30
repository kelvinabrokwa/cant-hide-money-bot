import asyncio
from typing import Dict, List

import numpy
import pandas

from cant_hide_money_bot.marketdata import MarketData
from cant_hide_money_bot.std import Symbol, Trader

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
MULT = 'mult'
PORTFOLIO = 'Portfolio'
POSITION = 'position'
QTY = 'qty'
SHARES = 'shares'
SYMBOL = 'symbol'
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


def filter_book_for_trader(book: pandas.DataFrame, trader: Trader) -> pandas.DataFrame:
    return book[book[TRADER] == trader]


def compute_shares_and_dollars(book: pandas.DataFrame) -> pandas.DataFrame:
    book[MULT] = book[DIR].apply(lambda dir_: 1 if dir_ == BUY else -1)
    book[SHARES] = book[QTY] * book[MULT]
    # You get negative dollars when you go long and positive dollars when you sell short
    book[DOLLARS] = book[QTY] * book[TRADE_PRICE] * (-1 * book[MULT])
    return book


def position_for_symbol(book: pandas.DataFrame, trader: Trader, symbol: Symbol) -> int:
    book = compute_shares_and_dollars(book)
    book = book[(book[TRADER] == trader) & (book[SYMBOL] == symbol)]
    return book[SHARES].sum()


def usd_for_trader(book: pandas.DataFrame, trader: Trader) -> float:
    book = compute_shares_and_dollars(book)
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
    # Calculate number of uninvented dollars
    usd = book[DOLLARS].sum() + usd_init
    # Calculate the value of the portfolio
    value = book[USD].sum() + usd_init
    # Determine whether we are LONG or SHORT by position
    book[POSITION] = book[SHARES].apply(lambda pos: 'LONG' if pos >= 0 else 'SHORT')
    # Filter out symbols with position = 0
    book = book[book[SHARES] != 0]
    # Select the columns we want to display
    book = book[[SYMBOL, SHARES, VALUE, AVG_COST, CURRENT_PRICE, POSITION]]
    # Add the USD and Portfolio columns
    book = book.append([
        {
            SYMBOL: USD_SYMBOL,
            SHARES: 0,
            VALUE: usd,
            AVG_COST: numpy.nan,
            CURRENT_PRICE: numpy.nan,
            POSITION: ''
        },
        {
            SYMBOL: PORTFOLIO,
            SHARES: 0,
            VALUE: value,
            AVG_COST: numpy.nan,
            CURRENT_PRICE: numpy.nan,
            POSITION: ''
        },
    ])

    return book


def get_all_symbols_with_non_zero_position(book_with_shares: pandas.DataFrame) -> List[str]:
    book = book_with_shares.groupby(SYMBOL, as_index=False).agg({SHARES: 'sum'})
    book = book[book[SHARES] != 0]
    return book[SYMBOL].unique()


async def gather_with_concurrency(n: int, *tasks):
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


async def get_current_prices(symbols, market_data: MarketData) -> pandas.DataFrame:
    df = pandas.DataFrame(data=symbols, columns=[SYMBOL])
    df[CURRENT_PRICE] = None

    async def fetch_current_price(d, symbol):
        symbol_data = await market_data.get_symbol_data(symbol, True)
        d.loc[d[SYMBOL] == symbol, CURRENT_PRICE] = symbol_data.mid()

    await gather_with_concurrency(30, *(fetch_current_price(df, symbol) for symbol in symbols))

    return df


async def all_portfolios(book: pandas.DataFrame, market_data: MarketData) -> Dict[str, pandas.DataFrame]:
    book_with_shares_and_dollars = compute_shares_and_dollars(book)
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
