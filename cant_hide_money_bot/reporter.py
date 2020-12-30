"""
This module is a script that send a summary to Guild that have this bot.
Scheduling is done externally (with cron, for example).
"""

import logging
import os
from datetime import datetime

import click
import discord
import dotenv

from cant_hide_money_bot import utils
from .book import all_portfolios, filter_book_for_guild_id
from .bot_common import find_channel, get_setting
from .marketdata import MarketData
from .std import Mode
from .store import Store

logging.basicConfig(format='%(asctime)-15s %(message)s', level=logging.INFO)

dotenv.load_dotenv()
DISCORD_TOKEN = os.environ['DISCORD_TOKEN']
RAPID_API_KEY = os.environ['RAPID_API_KEY']
DEV_GUILD_ID = int(os.environ['DEV_GUILD_ID'])

client = discord.Client()

MODE: Mode


@client.event
async def on_ready():
    market_data = MarketData(RAPID_API_KEY, MODE)
    store = Store(MODE)
    book = store.load_book()
    settings = store.load_settings()
    day = datetime.today().strftime('%A')

    for guild in client.guilds:
        logging.info(f'{client.user} connected to: {guild.name}(id: {guild.id})')

        # In dev mode, only send messages to the dev guild
        if MODE is Mode.DEV and guild.id != DEV_GUILD_ID:
            logging.info(f'DEV mode -- skipping {guild.name}(id: {guild.id})')
            continue

        # Determine which channel to send to
        if (channel_name := get_setting(settings, guild.id, 'channel')) is None:
            channel_name = 'general'
        channel = find_channel(guild, channel_name)
        if channel is None:
            raise Exception('Could not find a channel for sending the report')

        book = filter_book_for_guild_id(book, guild.id)
        portfolios = await all_portfolios(book, market_data)
        await channel.send(f"Happy {day}, traders! Here's how we're doing:")
        if len(portfolios):
            for trader, portfolio in portfolios.items():
                image_path = utils.df_to_image(portfolio, title=trader)
                if image_path is not None:
                    await channel.send(file=discord.File(image_path))
        else:
            channel.send('No positions -- get busy!')

        await channel.send('Good luck out there!')

    os._exit(0)


@click.command()
@click.option('--mode', type=click.Choice(['dev', 'prod']), required=True)
def main(mode: str) -> None:
    global MODE

    MODE = Mode[mode.upper()]
    client.run(DISCORD_TOKEN)


if __name__ == '__main__':
    main()
