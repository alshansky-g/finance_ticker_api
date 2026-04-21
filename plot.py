import plotly.graph_objects as go
from config_logging import get_logger
from csv_parser import get_tickers_info_for_plot

logger = get_logger(__name__)


def build_chart(tickers_info: dict[str, tuple[list[str], list[float]]]) -> go.Figure:
    fig = go.Figure()

    for ticker, (dates, prices) in tickers_info.items():
        fig.add_scatter(x=dates, y=prices, mode="lines", name=ticker)

    fig.update_layout(
        title="График скорректированных цен (нормализация к 100)",
        xaxis_title="Дата",
        yaxis_title="% от стартовой цены",
        template="plotly_dark",
        hovermode="closest",
    )
    return fig


def show_chart(tickers_info):
    if tickers_info:
        build_chart(tickers_info).show()
    else:
        logger.info("Нечего отображать. Нет информации по тикерам")


if __name__ == "__main__":
    tickers_info = get_tickers_info_for_plot()
    show_chart(tickers_info)
