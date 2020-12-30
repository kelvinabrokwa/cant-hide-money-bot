from pyrsistent import pmap

from cant_hide_money_bot.bot_common import set_setting


def test_set_setting():
    settings = pmap()
    guild = 8888
    key = 'channel'
    value = 'm2p-capital'
    new_settings = set_setting(settings, guild, key, value)
    assert new_settings[guild][key] == value
