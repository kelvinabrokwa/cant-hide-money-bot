import functools
import tempfile

import imgkit
import matplotlib
import numpy as np
import pandas as pd
from PIL import Image

from . import book


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


imgkit_options = {'quiet': '', 'width': 750, 'disable-smart-width': ''}

td_and_th_props = [
    ('border', '1px solid transparent'),
    ('height', '30px')
]
just_th_props = [
    ('background', '#DFDFDF'),
    ('font-weight', 'bold')
]
just_td_props = [
    ('text-align', 'center')
]
alt_props = [
    ('background-color', 'white')
]
table_styles = [
    dict(selector='td, th', props=td_and_th_props),
    dict(selector='th', props=just_th_props),
    dict(selector='td', props=just_td_props),
    dict(selector='table tr:nth-child(even) td', props=alt_props),
]


def trades_to_table(df):
    df = df[[book.SYMBOL, book.DIR, book.QTY, book.TRADE_PRICE, book.TRADER, book.TIME]]
    dollar_format = '${:20,.2f}'.format
    df[book.TRADE_PRICE] = df[book.TRADE_PRICE].map(dollar_format)
    styler = df.reset_index(drop=True).style.hide_index().set_table_styles(table_styles)
    html = styler.render()
    fd, filename = tempfile.mkstemp(suffix='.png')
    imgkit.from_string(html, filename, options={'quiet': '', 'width': 600, 'disable-smart-width': ''})
    return filename


def df_to_table(df, title=None):
    df = df.copy(deep=True)

    styler = df.reset_index(drop=True).style
    styler.hide_index()

    def format_maybe_nan(template):
        def f(n):
            if np.isnan(n):
                return ''
            else:
                return template.format(n)
        return f

    dollar_format = format_maybe_nan('${:20,.2f}')

    styler.format(formatter={
        book.SHARES: format_maybe_nan('{:.0f}'),
        book.VALUE: dollar_format,
        book.AVG_COST: dollar_format,
        book.CURRENT_PRICE: dollar_format,
        book.MARK_PNL: dollar_format,
        book.RETURN: format_maybe_nan('{:5,.2f}%')
    })
    styler.set_table_styles(table_styles)
    if title is not None:
        styler = styler.set_caption(title)

    def style_negative(value):
        if value < 0:
            return 'background-color:#ffa1a1'
        else:
            return None

    styler.applymap(style_negative, subset=pd.IndexSlice[:, [book.MARK_PNL, book.RETURN]])

    html = styler.render()
    fd, filename = tempfile.mkstemp(suffix='.png')
    imgkit.from_string(html, filename, options=imgkit_options)
    return filename


def format_df_for_chart(df):
    # Matplotlib doesn't like Infs and NaNs
    df = df.replace([np.inf, -np.inf], np.nan).dropna()

    # Remove the Portfolio and USD rows
    df = df[(df[book.SYMBOL] != book.PORTFOLIO) & (df[book.SYMBOL] != book.USD_SYMBOL)]

    return df


def df_to_exposure_plot(df):
    df = df.copy(deep=True)
    df = format_df_for_chart(df)

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
    matplotlib.pyplot.title('Exposure')
    _fd, chart_filename = tempfile.mkstemp(suffix='.png')
    plot.figure.savefig(chart_filename)

    return chart_filename


def df_to_return_plot(df):
    df = df.copy(deep=True)
    df = format_df_for_chart(df)

    # Only create the plot if there are rows for security positions
    if not len(df.index):
        return None

    # Sort and create a bar chart
    plot = df.sort_values(book.RETURN).plot.barh(x=book.SYMBOL, y=book.RETURN, rot=0)
    plot.get_xaxis().set_major_formatter(matplotlib.ticker.PercentFormatter())
    matplotlib.pyplot.xticks(rotation=25)
    matplotlib.pyplot.tick_params(axis='y', which='major', labelsize=6)
    matplotlib.pyplot.title('Return')
    _fd, chart_filename = tempfile.mkstemp(suffix='.png')
    plot.figure.savefig(chart_filename)
    matplotlib.pyplot.close()

    return chart_filename


def df_to_image(df, title=None) -> str:
    table_filename = df_to_table(df, title=title)
    exposure_plot_filename = df_to_exposure_plot(df)
    return_plot_filename = df_to_return_plot(df)

    # Stitch images together
    images = [
        Image.open(table_filename),
        Image.open(exposure_plot_filename),
        Image.open(return_plot_filename)
    ]
    widths, heights = zip(*(i.size for i in images))
    max_width = max(widths)
    total_height = sum(heights)
    new_image = Image.new('RGB', (max_width, total_height))
    y_offset = 0
    for image in images:
        new_image.paste(image, (0, y_offset))
        y_offset += image.size[1]

    _fd, image_filename = tempfile.mkstemp(suffix='.png')
    new_image.save(image_filename)

    return image_filename
