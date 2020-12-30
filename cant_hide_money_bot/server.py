"""
This module implements the bot that communicates with Discord
"""

import asyncio
import logging
import math
import os
import random
from datetime import datetime
from functools import wraps
from typing import Optional, Union

import click
import discord
import dotenv
import pandas
from discord.ext import commands
from pyrsistent import pmap
from pyrsistent.typing import PMap

from cant_hide_money_bot.book import all_portfolios, filter_book_for_guild_id, filter_book_for_trader, \
    position_for_symbol, usd_for_trader
from . import all_symbols, lessons, utils
from .bot_common import get_setting, set_setting
from .marketdata import MarketData
from .std import Dir, Dollars, Guild_id, Mode, Shares, Symbol, Trade, TradeError, Trader, md
from .store import Store

logging.basicConfig(format='%(asctime)-15s %(message)s', level=logging.INFO)

# Global variables that are set when this module is initialized
# Maybe this should be a class so that initialization is done
# explicitly via constructor
MODE: Mode
DEV_GUILD_ID: Guild_id
BOOK: pandas.DataFrame
SETTINGS: PMap[Guild_id, PMap[str, str]]
STORE: Store
MARKET_DATA: MarketData

# This lock is used to synchronize mutations to BOOK (the global variable)
BOOK_LOCK = asyncio.Lock()

bot = commands.Bot('!', description="~if you ain't talkin money i ain't talkin~")


async def create_trade(symbol: Symbol, qty: Union[Shares, Dollars], dir_: Dir, trader: Trader, guild_id: Guild_id,
                       time: datetime, clamp_qty: bool = False) -> Trade:
    """
    :clamp_qty: We do not allow trading less than 1 share or more that total volume / 2. When clamp_qty is True, we will
    automatically limit quantities by those constraints (1 if qty < 1 and volume / 2 if greater). If clamp_qty is False,
    we throw when outside of those bounds.
    """
    symbols_data = await MARKET_DATA.get_symbols_data([symbol], False)

    symbol_data = symbols_data.get(symbol)
    if symbol_data is None:
        raise TradeError(f'could not get market data for symbol: {symbol}')

    # We're not market-making so we just cross the spread and take
    if dir_ == Dir.BUY:
        price = symbol_data.ask
    elif dir_ == Dir.SELL:
        price = symbol_data.bid
    else:
        raise TradeError(f'unrecognized direction: {dir_}')

    # Convert dollars to shares
    if type(qty) == Dollars:
        shares = math.floor(qty / price)
        qty = Shares(shares)

    if symbol_data.currency != 'USD':
        raise TradeError(
            f'Sorry you cannot trade this symbol. It trades in {symbol_data.currency}. We only trade in USD.')

    qty_is_invalid = qty < 1 or ((symbol_data.volume is not None) and (qty > (symbol_data.volume / 2.)))

    if qty_is_invalid:
        if not clamp_qty:
            raise TradeError(
                f'You cannot trade less than 1 or more than half the volume of this symbol. The volume of this symbol '
                f'is {symbol_data.volume}.')
        elif qty > (symbol_data.volume / 2.):
            qty = int(symbol_data.volume / 2.)
        elif qty < 1:
            qty = 1

    return Trade(
        symbol=symbol,
        dir_=dir_,
        qty=qty,
        time=time,
        price=price,
        trader=trader,
        guild_id=guild_id)


async def handle_trade(symbol: Symbol, qty: Union[Shares, Dollars], dir_: Dir, trader: Trader, guild,
                       clamp_qty: bool = False) -> str:
    """
    Create a trade and put it on the book
    """
    global BOOK

    # Create the trade
    try:
        trade = await create_trade(symbol, qty, dir_, trader, guild.id, datetime.now(), clamp_qty=clamp_qty)
    except TradeError as e:
        return str(e)

    async with BOOK_LOCK:
        # Check that the trade doesn't result in the trader having negative dollars
        book_with_new_trade = BOOK.append([utils.dict_of_trade(trade)])
        resulting_usd = usd_for_trader(book_with_new_trade, trader)
        if resulting_usd < 0:
            return f'This trade would result in you having ${resulting_usd:.2f}. You can not be short USD.'

        # Persist the trade
        STORE.persist_trade(trade)

        # Execute the trade by adding it to the book
        BOOK = book_with_new_trade

    bought_or_sold = 'BOUGHT' if dir_ == Dir.BUY else 'SOLD'
    total_price = trade.qty * trade.price
    response = f'{bought_or_sold} {int(trade.qty)} {trade.symbol} @ ${trade.price} (${total_price:.2f})'
    logging.info(response)
    return response


def mode_check(f):
    """
    A decorator that ignores commands from all non-dev guilds when in dev mode
    """

    @wraps(f)
    async def wrapper(ctx, *args, **kwargs):
        STORE.update_trader_info(ctx.author.id, ctx.author.name, str(ctx.author), ctx.guild.id)
        if (MODE is Mode.PROD) or (ctx.guild.id == DEV_GUILD_ID):
            return await f(ctx, *args, **kwargs)

    return wrapper


@bot.command(name='T', help='List trades')
@mode_check
async def trades(ctx) -> None:
    image_path = utils.trades_to_table(BOOK)
    await ctx.send(file=discord.File(image_path))


async def send_trader_portfolio(ctx) -> None:
    trader = str(ctx.author)
    book = filter_book_for_guild_id(BOOK, ctx.guild.id)
    book = filter_book_for_trader(book, trader)
    portfolios = await all_portfolios(book, MARKET_DATA)
    if (portfolio := portfolios.get(trader)) is not None:
        image_path = utils.df_to_image(portfolio, title=trader)
        if image_path is not None:
            await ctx.send(file=discord.File(image_path))
    else:
        await ctx.send('No positions -- get busy!')


@bot.command(name='$', help='Print your own portfolio')
@mode_check
async def trader_portfolio(ctx) -> None:
    await send_trader_portfolio(ctx)


@bot.command(name='$$', help='Print all portfolios per trader and the fund portfolio')
@mode_check
async def all_portfolios_(ctx) -> None:
    b = filter_book_for_guild_id(BOOK, ctx.guild.id)
    portfolios = await all_portfolios(b, MARKET_DATA)
    if len(portfolios):
        for trader, portfolio in portfolios.items():
            image_path = utils.df_to_image(portfolio, title=trader)
            if image_path is not None:
                await ctx.send(file=discord.File(image_path))
    else:
        ctx.send('No positions -- get busy!')


def parse_qty(qty: str):
    qty = qty.replace(',', '')

    if not len(qty):
        raise ValueError('empty string passed as qty')

    if qty[0] == '$':
        return Dollars(float(qty[1:]))
    else:
        return Shares(float(qty))


async def parse_qty_and_send_error_message(qty, ctx):
    try:
        return parse_qty(qty)
    except ValueError as e:
        await ctx.send(f'Invalid quantity "{qty}". Do you have the arguments in the correct order?')
        raise e


@bot.command(name='BUY', help='Buy some shares')
@mode_check
async def buy(ctx, symbol: str, qty: str) -> None:
    """Here and in sell below we don't type annotate qty because we want to send a useful message when its not a float
    """
    symbol = Symbol(symbol)
    qty = await parse_qty_and_send_error_message(qty, ctx)
    trader = Trader(ctx.author)
    response = await handle_trade(symbol, qty, Dir.BUY, trader, ctx.guild)
    await ctx.send(response)
    await send_trader_portfolio(ctx)


@bot.command(name='SELL', help='Sell some shares')
@mode_check
async def sell(ctx, symbol: str, qty: str) -> None:
    symbol = Symbol(symbol)
    qty = await parse_qty_and_send_error_message(qty, ctx)
    trader = Trader(ctx.author)
    response = await handle_trade(symbol, qty, Dir.SELL, trader, ctx.guild)
    await ctx.send(response)
    await send_trader_portfolio(ctx)


@bot.command(name='CLOSE', help='Sell some shares')
@mode_check
async def close(ctx, symbol: str) -> None:
    trader = Trader(ctx.author)
    symbol = Symbol(symbol)
    current_position = position_for_symbol(BOOK, trader, symbol)

    if current_position == 0:
        await ctx.send(f'You do not have a position in {symbol}')
        return

    dir_ = Dir.BUY if current_position < 0 else Dir.SELL
    qty = Shares(abs(current_position))
    response = await handle_trade(symbol, qty, dir_, trader, ctx.guild)
    await ctx.send(response)
    await send_trader_portfolio(ctx)


@bot.command(name='IM-FEELING-LUCKY', help='Buy or sell a random quantity of a random symbol')
@mode_check
async def im_feeling_lucky(ctx) -> None:
    symbol = Symbol(all_symbols.random_symbol())
    qty = Shares(random.randint(1, 10000))
    dir_ = random.choice([Dir.BUY, Dir.SELL])
    trader = Trader(ctx.author)
    response = await handle_trade(symbol, qty, dir_, trader, ctx.guild, clamp_qty=True)
    await ctx.send(response)
    await send_trader_portfolio(ctx)


@bot.command(name='SETTINGS', help="See your server's settings")
@mode_check
async def get_settings(ctx) -> None:
    guild_settings = SETTINGS.get(ctx.guild.id, pmap())
    lines = [f'{key} = {value}' for key, value in guild_settings.items()]
    message = '\n'.join(lines)
    await ctx.send(md(message))


@bot.command(name='SET-SETTING', help='Set a setting')
@mode_check
async def set_setting_(ctx, key: str, value: str) -> None:
    global SETTINGS
    guild_id = ctx.guild.id
    SETTINGS = set_setting(SETTINGS, guild_id, key, value)
    STORE.set_setting(guild_id, key, value)
    await ctx.send(md(f'Updated settings: {key} = {value}'))


def set_money_message_settings(guild_id, value):
    global SETTINGS
    key = 'money_message'
    value = str(value)
    SETTINGS = set_setting(SETTINGS, guild_id, key, value)
    STORE.set_setting(guild_id, key, value)


@bot.command(name='DISABLE-MONEY-MESSAGE', help='Disable the "did someone say...money ??" message')
@mode_check
async def disable_money_message(ctx) -> None:
    set_money_message_settings(ctx.guild.id, False)
    await ctx.send('Disabled money message')


@bot.command(name='ENABLE-MONEY-MESSAGE', help='Enable the "did someone say...money ??" message')
@mode_check
async def disable_money_message(ctx) -> None:
    set_money_message_settings(ctx.guild.id, True)
    await ctx.send('Enabled money message')


@bot.command(name='ELI5', help='A quick lesson on trading')
@mode_check
async def eli5(ctx) -> None:
    await ctx.send(md(lessons.eli5))


@bot.event
async def on_ready() -> None:
    logging.info(f'{bot.user} has connected to Discord!')
    for guild in bot.guilds:
        logging.info(f'{bot.user} connected to: {guild.name}(id: {guild.id})')


def find_money_word(message: str) -> Optional[str]:
    money_words = ['money', 'bread', 'dough', 'cheese', 'cheddar',
                   'bones', 'clams', 'guap', 'moola', 'smackers', 'bands', 'paper']
    return next((word for word in money_words if word in message.lower()), None)


def str_to_bool(s):
    return s.lower() == 'true'


@bot.event
async def on_message(message) -> None:
    # The bot should not react to its own messages
    if message.author == bot.user:
        return

    money_message_enabled = get_setting(SETTINGS, message.guild.id, 'money_message', f=str_to_bool, default=True)
    if money_message_enabled:
        channel_setting = get_setting(SETTINGS, message.guild.id, 'channel')
        if channel_setting == message.channel.name:
            # When someone mentions money in the cant-hide-money-bot-designated channel, assert your presence
            if (money_word := find_money_word(message.content)) is not None:
                await message.channel.send(f'did someone say...{money_word} ?? 👀')

    await bot.process_commands(message)


@click.command()
@click.option('--mode', type=click.Choice(['dev', 'prod']), required=True)
def main(mode) -> None:
    global DEV_GUILD_ID
    global MODE
    global BOOK
    global STORE
    global SETTINGS
    global MARKET_DATA

    dotenv.load_dotenv()
    token = os.environ['DISCORD_TOKEN']
    rapid_api_key = os.environ['RAPID_API_KEY']

    # Set globals
    DEV_GUILD_ID = int(os.environ['DEV_GUILD_ID'])
    MODE = Mode[mode.upper()]
    MARKET_DATA = MarketData(rapid_api_key, MODE)
    STORE = Store(MODE)
    BOOK = STORE.load_book()
    SETTINGS = STORE.load_settings()

    # Start the bot
    bot.run(token)


if __name__ == '__main__':
    main()
