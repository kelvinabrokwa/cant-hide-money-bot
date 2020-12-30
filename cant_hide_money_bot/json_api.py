"""An API for querying data as JSON
"""

import pandas
import flask
import flask_cors

from . import book, std, marketdata
from .store import Store

app = flask.Flask(__name__)
flask_cors.CORS(app)


RAPID_API_KEY = None
MODE = std.Mode.PROD

MARKET_DATA = marketdata.MarketData(RAPID_API_KEY, MODE)
STORE = Store(MODE)


@app.route('/')
async def heartbeat():
    return flask.jsonify('ok')


@app.route('/portfolios')
async def portfolios():
    book_ = STORE.load_book()
    portfolios = await book.all_portfolios(book_, MARKET_DATA)
    portfolios = {
        # Convert NaNs to Nones and then convert to dict
        trader: portfolio.where(pandas.notnull(portfolio), None).to_dict(orient='records')
        for trader, portfolio in portfolios.items()}
    return flask.jsonify(portfolios)


@app.route('/trades/<int:guild_id>')
async def trades(guild_id):
    book_ = STORE.load_book()
    book_ = book_.loc[book_[book.GUILD_ID] == guild_id]
    return flask.jsonify(book_.to_dict(orient='records'))
