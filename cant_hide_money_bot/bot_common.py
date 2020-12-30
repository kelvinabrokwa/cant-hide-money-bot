"""
Code shared amongst various bot scripts
"""

from typing import Optional

from pyrsistent import pmap

from .std import Guild_id, Settings


def get_setting(settings: Settings, guild_id: Guild_id, key: str, f=None, default=None) -> Optional[str]:
    value = settings.get(guild_id, pmap()).get(key)

    if value is None:
        return default

    if f is not None:
        return f(value)

    return value


def set_setting(settings: Settings, guild_id: Guild_id, key: str, value: str) -> Settings:
    new_guild_settings = settings.get(guild_id, pmap()).set(key, value)
    return settings.set(guild_id, new_guild_settings)


def find_channel(guild, channel_name: str):
    return next((channel for channel in guild.channels if channel.name == channel_name), None)
