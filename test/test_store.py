import sqlite3
from datetime import datetime

from cant_hide_money_bot.std import Dir, Mode, Shares, Symbol, Trade
from cant_hide_money_bot.store import Store
from cant_hide_money_bot.utils import dict_of_trade

sqlite3.enable_callback_tracebacks(True)


def test_trade():
    guild_id = 100
    store = Store(Mode.DEV, in_memory=True)
    inserted_trade = Trade(symbol=Symbol('ZVZZT'), dir_=Dir.BUY, qty=Shares(100), price=100,
                           time=datetime.fromisoformat('2020-01-01T00:09:30'), trader='kelvin', guild_id=guild_id)
    store.persist_trade(inserted_trade)
    book = store.load_book()
    loaded_trade = book.loc[0].to_dict()
    assert dict_of_trade(inserted_trade) == loaded_trade


def test_setting():
    store = Store(Mode.DEV, in_memory=True)
    guild = 8888
    key = 'channel'
    old_value = 'general'
    new_value = 'm2p-capital'
    store.set_setting(guild, key, old_value)
    store.set_setting(guild, key, new_value)
    settings = store.load_settings()
    assert settings[guild][key] == new_value
