from collections.abc import Iterator
from datetime import UTC, datetime, timezone
import requests
from rich import print_json


TICKERS_FILE = "tickers.txt"
BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/"
NOW = datetime.now(UTC).strftime("%d.%m.%y")


def get_ticker(file: str) -> Iterator[str]:
    with open(file, encoding="utf-8") as tickers:
        for ticker in tickers:
            yield ticker.strip()


def get_history_data(
    *, ticker: str, start_date: str, end_date: str, interval: str = "1wk"
):
    """
    Получает исторические данные для указанного тикера актива.

    :param ticker: str, тикер актива.
    :param start_date: str, дата начала периода в формате 'дд.мм.гг'.
    :param end_date: str, дата окончания периода в формате 'дд.мм.гг'.
    :param interval: str, интервал времени (неделя, день и т.д.) (необязательный, по умолчанию '1wk' - одна неделя).
    :return: str, JSON-строка с историческими данными.
    """
    per2 = int(
        datetime.strptime(end_date, "%d.%m.%y").replace(tzinfo=timezone.utc).timestamp()
    )
    per1 = int(
        datetime.strptime(start_date, "%d.%m.%y")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )
    params = {
        "period1": str(per1),
        "period2": str(per2),
        "interval": interval,
        "includeAdjustedClose": "true",
    }
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, params=params)
    return response.json()


for ticker in get_ticker(TICKERS_FILE):
    print_json(
        data=get_history_data(
            ticker=ticker,
            start_date="01.01.20",
            end_date=NOW,
        )
    )
    break
