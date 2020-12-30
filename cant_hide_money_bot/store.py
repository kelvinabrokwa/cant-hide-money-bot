"""
This module provides functionality for serializing, deserializing,
loading, and persisting trades
"""

import logging
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import DefaultDict, Dict, Optional

import pandas
from pyrsistent import pmap

from .std import Guild_id, Mode, Settings, Trade

DEFAULT_DIR = Path.home() / '.cant-hide-money-bot'


def db_path(mode: Mode) -> str:
    db_name = Path(f'data.{mode.name.lower()}.db')
    return DEFAULT_DIR / db_name


@contextmanager
def db_conn(db):
    # In [in_memory] mode, you have to use a single connection throughout
    # but in regular mode, you should not since this is a multithreaded application
    if type(db) == sqlite3.Connection:
        yield db
    else:
        conn = sqlite3.connect(db)
        try:
            with conn:
                yield conn
        finally:
            conn.close()


class Store:
    def __init__(self, mode: Mode, in_memory=False) -> None:
        if in_memory:
            self.db = sqlite3.connect('file::memory:?cache=shared')
        else:
            DEFAULT_DIR.mkdir(parents=True, exist_ok=True)
            self.db = db_path(mode)
            logging.info(f'using database: {self.db}')

        logging.info('initializing database')
        create_trades_table = '''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                dir TEXT NOT NULL,
                qty INTEGER NOT NULL,
                time TEXT NOT NULL,
                price REAL NOT NULL,
                trader TEXT NOT NULL,
                guild_id INTEGER NOT NULL)
        '''
        create_settings_table = '''
            CREATE TABLE IF NOT EXISTS settings (
                guild INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                set_at DATETIME DEFAULT CURRENT_TIMESTAMP)
        '''
        create_traders_table = '''
            CREATE TABLE IF NOT EXISTS traders (
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                name_and_id TEXT NOT NULL,
                guild_id INTEGER NOT NULL,
                UNIQUE(id, guild_id))
        '''
        with db_conn(self.db) as conn:
            cursor = conn.cursor()
            cursor.execute(create_trades_table)
            cursor.execute(create_settings_table)
            cursor.execute(create_traders_table)

    def load_book(self) -> pandas.DataFrame:
        query = '''
            SELECT symbol, dir, qty, time, price, trader, guild_id
            FROM trades
        '''
        with db_conn(self.db) as conn:
            return pandas.read_sql(query, conn, parse_dates=['time'])

    def persist_trade(self, trade: Trade) -> None:
        query = '''
            INSERT INTO trades (symbol, dir, qty, time, price, trader, guild_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
        values = (
            trade.symbol,
            trade.dir_.name,
            trade.qty,
            str(trade.time),
            trade.price,
            trade.trader,
            trade.guild_id,
        )
        with db_conn(self.db) as conn:
            cursor = conn.cursor()
            cursor.execute(query, values)

    def set_setting(self, guild: Guild_id, key: str, value: str):
        query = '''
            INSERT INTO settings (guild, key, value)
            VALUES (?, ?, ?)
        '''
        values = (guild, key, value)
        with db_conn(self.db) as conn:
            cursor = conn.cursor()
            cursor.execute(query, values)

    def get_setting(self, guild: str, key: str) -> Optional[str]:
        query = '''
            SELECT value
            FROM settings
            WHERE guild = ? AND key = ?
        '''
        with db_conn(self.db) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (guild, key))
            return cursor.fetchone()

    def load_settings(self) -> Settings:
        # Select the newest value for each distinct (guild, key) pair
        query = '''
            SELECT a.guild, a.key, a.value
            FROM settings a
            INNER JOIN (
                SELECT guild, key, MAX(set_at) set_at
                FROM settings
                GROUP BY guild, key
            ) b
            ON a.guild = b.guild AND a.key = b.key AND a.set_at = b.set_at;
        '''
        settings: DefaultDict[Guild_id, Dict[str, str]] = defaultdict(dict)
        with db_conn(self.db) as conn:
            cursor = conn.cursor()
            for guild, key, value in cursor.execute(query):
                settings[guild][key] = value
        return pmap({guild: pmap(guild_settings) for guild, guild_settings in settings.items()})

    def update_trader_info(self, id_, name, name_and_id, guild_id) -> None:
        query = '''
            INSERT INTO traders (id, name, name_and_id, guild_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (id, guild_id) DO UPDATE SET name = ?, name_and_id = ?
        '''
        values = (id_, name, name_and_id, guild_id, name, name_and_id)
        with db_conn(self.db) as conn:
            cursor = conn.cursor()
            cursor.execute(query, values)
