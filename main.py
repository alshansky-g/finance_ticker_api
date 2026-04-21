from sys import argv
from api_parser import parse_tickers_api
from csv_parser import get_tickers_info_for_plot
from plot import show_chart


if len(argv) > 1:
    parse_tickers_api()

tickers_info = get_tickers_info_for_plot()
show_chart(tickers_info)
