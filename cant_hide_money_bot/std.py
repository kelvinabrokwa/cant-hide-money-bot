"""
This module contains the core types and classes used throughout the app
"""
import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, unique
from typing import Any, Dict, Optional

from pyrsistent.typing import PMap, PVector


class Symbol(str):
    def __new__(cls, content):
        return super().__new__(cls, content.upper())


@dataclass(frozen=True)
class SymbolData:
    bid: float
    ask: float
    volume: Optional[int]
    currency: str

    def mid(self):
        return (self.bid + self.ask) / 2.


@unique
class Dir(Enum):
    BUY = 1
    SELL = 2


class Shares(float):
    pass


class Dollars(float):
    pass


Trader = str

Guild_id = int

Settings = PMap[Guild_id, PMap[str, str]]


@dataclass(frozen=True)
class Trade:
    symbol: Symbol
    dir_: Dir
    qty: Shares
    time: datetime  # The time that this trade was executed
    price: float  # The price of [symbol] at [time]
    trader: Trader  # The trader who did the trade
    guild_id: Guild_id


Book = PVector[Trade]


@unique
class Mode(Enum):
    DEV = 1
    PROD = 2


class TradeError(Exception):
    pass


class NoPositionsError(Exception):
    pass


USD = Symbol('USD')


class TimedCache:
    """
    A cache that invalidates entries after [max_age_seconds]
    """

    def __init__(self, max_age_seconds) -> None:
        self._max_age_seconds = max_age_seconds
        self._cache: Dict[Any, Any] = {}
        self.lock = asyncio.Lock()

    def put(self, key, value) -> None:
        self._cache[key] = (value, time.time())

    def get(self, key):
        self._purge()
        if (value_and_time := self._cache.get(key)) is not None:
            return value_and_time[0]
        return None

    def _purge(self):
        current_time = time.time()
        self._cache = {key: (value, insert_time) for key, (value, insert_time) in self._cache.items()
                       if (current_time - insert_time) < self._max_age_seconds}


def dir_to_mult(dir_: Dir) -> int:
    if dir_ == Dir.BUY:
        return 1
    elif dir_ == Dir.SELL:
        return -1
    else:
        raise ValueError(f'unsupported direction: {dir_}')


def md(text: str) -> str:
    return f'```{text}```'
