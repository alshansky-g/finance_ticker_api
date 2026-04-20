from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from csv import DictWriter
from datetime import UTC, datetime
from pathlib import Path
from queue import Queue
from threading import Thread
import requests
from config_logging import get_logger
from schemas import Ticker


logger = get_logger(__name__)

TICKERS_FILE = "tickers.txt"
BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/"
START_DATE = "01.01.20"
END_DATE = datetime.now(UTC).strftime("%d.%m.%y")


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
    per2 = int(datetime.strptime(end_date, "%d.%m.%y").replace(tzinfo=UTC).timestamp())
    per1 = int(
        datetime.strptime(start_date, "%d.%m.%y").replace(tzinfo=UTC).timestamp()
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


def worker(ticker: str, queue: Queue):
    try:
        ticker_model = Ticker.model_validate(
            get_history_data(ticker=ticker, start_date=START_DATE, end_date=END_DATE)
        )
        queue.put(ticker_model)
    except requests.RequestException as e:
        logger.error("Проблема с сетью при обработке %s: %s", ticker_model.symbol, e)
    except Exception as e:
        logger.error("Не получилось обработать %s: %s", ticker_model.symbol, e)


def csv_writer(queue: Queue):
    Path("tickers").mkdir(parents=True, exist_ok=True)
    while True:
        try:
            model: Ticker = queue.get()
            with open(f"tickers/{model.symbol}.csv", "w", newline="") as file:
                writer = DictWriter(file, fieldnames=model.headers)
                writer.writeheader()
                writer.writerows(model.to_rows())
                logger.info('%s записан в tickers/%s.csv', model.symbol, model.symbol)
        finally:
            queue.task_done()


def main():
    work_queue = Queue()
    Thread(target=csv_writer, args=[work_queue], daemon=True).start()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(worker, ticker, work_queue)
            for ticker in get_ticker(TICKERS_FILE)
        ]
        for f in futures:
            f.result()
    work_queue.join()


if __name__ == "__main__":
    main()
