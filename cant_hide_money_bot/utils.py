import tempfile

import imgkit
import matplotlib
import numpy as np

from cant_hide_money_bot import book


def dict_of_trade(trade):
    return {
        'symbol': trade.symbol,
        'dir': trade.dir_.name,
        'qty': trade.qty,
        'time': trade.time,
        'price': trade.price,
        'trader': trade.trader,
        'guild_id': trade.guild_id,
    }


imgkit_options = {'quiet': '', 'width': 600, 'disable-smart-width': ''}

td_and_th_props = [
    ('border', '1px solid transparent'),
    ('height', '30px')
]
just_th_props = [
    ('background', '#DFDFDF'),
    ('font-weight', 'bold')
]
just_td_props = [
    ('background', '#FAFAFA'),
    ('text-align', 'center')
]
alt_props = [
    ('background-color', 'white')
]
table_styles = [
    dict(selector='td, th', props=td_and_th_props),
    dict(selector='th', props=just_th_props),
    dict(selector='td', props=just_td_props),
    dict(selector='table tr:nth-child(odd) td', props=alt_props),
]


def df_to_table(df, title=None):
    dollar_format = '${:20,.2f}'.format
    df[book.SHARES] = df[book.SHARES].astype(int)
    df[book.VALUE] = df[book.VALUE].map(dollar_format)
    df[book.AVG_COST] = df[book.AVG_COST].map(dollar_format)
    df[book.CURRENT_PRICE] = df[book.CURRENT_PRICE].map(dollar_format)
    df.loc[(df[book.SYMBOL] == book.PORTFOLIO) | (df[book.SYMBOL] == book.USD_SYMBOL), book.SHARES] = ''
    df.loc[(df[book.SYMBOL] == book.PORTFOLIO) | (df[book.SYMBOL] == book.USD_SYMBOL), book.AVG_COST] = ''
    df.loc[(df[book.SYMBOL] == book.PORTFOLIO) | (df[book.SYMBOL] == book.USD_SYMBOL), book.CURRENT_PRICE] = ''
    styler = df.reset_index(drop=True).style.hide_index().set_table_styles(table_styles)
    if title is not None:
        styler = styler.set_caption(title)
    html = styler.render()
    fd, filename = tempfile.mkstemp(suffix='.png')
    imgkit.from_string(html, filename, options=imgkit_options)
    return filename


def df_to_plot(df):
    # Matplotlib doesn't like Infs and NaNs
    df = df.replace([np.inf, -np.inf], np.nan).dropna()

    # Remove the Portfolio and USD rows
    df = df[(df[book.SYMBOL] != book.PORTFOLIO) & (df[book.SYMBOL] != book.USD_SYMBOL)]

    # Only create the plot if there are rows for security positions
    if not len(df.index):
        return None

    # Sort and create a bar chart
    plot = df.sort_values(book.VALUE).plot.barh(x=book.SYMBOL, y=book.VALUE, rot=0)
    # Format dollar axis
    tick = matplotlib.ticker.StrMethodFormatter('${x:,.0f}')
    plot.get_xaxis().set_major_formatter(tick)
    matplotlib.pyplot.xticks(rotation=25)
    matplotlib.pyplot.tick_params(axis='y', which='major', labelsize=6)
    fd, chart_filename = tempfile.mkstemp(suffix='.png')
    plot.figure.savefig(chart_filename)

    return chart_filename


def df_to_images(df, title=None):
    # We copy because df_to_table mutates the table
    table_filename = df_to_table(df.copy(deep=True), title=title)
    plot_filename = df_to_plot(df)
    return table_filename, plot_filename
