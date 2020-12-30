import os
import sys

import click
import discord
import dotenv

dotenv.load_dotenv()
TEST_BOT_DISCORD_TOKEN = os.environ['TEST_BOT_DISCORD_TOKEN']
DEV_GUILD_ID = int(os.environ['DEV_GUILD_ID'])
DEV_GUILD_CHANNEL = os.getenv('DEV_GUILD_CHANNEL', 'general')

client = discord.Client()

COMMANDS = [
    '!BUY AAPL 100',
    '!SELL GOOG 200',
    '!$',
    '!$$',
    '!T',
    '!IM-FEELING-LUCKY',
    '!ELI5',
    '!SET_SETTING foo bar',
    '!SETTING',
    '!help',
]


@client.event
async def on_ready():
    print(list(guild.id for guild in client.guilds))
    guild = next(guild for guild in client.guilds if guild.id == DEV_GUILD_ID)
    channel = next(channel for channel in guild.channels if channel.name == DEV_GUILD_CHANNEL)
    for command in COMMANDS:
        await channel.send(command)
    sys.exit(0)


@click.command()
def main() -> None:
    client.run(TEST_BOT_DISCORD_TOKEN)


if __name__ == '__main__':
    main()
