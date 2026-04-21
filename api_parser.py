from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from csv import DictWriter
from datetime import UTC, datetime
from pathlib import Path
from queue import Queue
from threading import Thread
import requests
import requests.adapters
from config import get_config
from config_logging import get_logger
from schemas import Ticker
from urllib3.util.retry import Retry


logger = get_logger(__name__)
config = get_config()

session = requests.Session()
retry = Retry(
    total=3,
    backoff_factor=1,
    backoff_jitter=0.5,
    respect_retry_after_header=True,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = requests.adapters.HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)


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
    url = f"{config.base_url}{ticker}"
    response = session.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        params=params,
        timeout=(5, 15),
    )
    return response.json()


def ticker_parser(*, ticker_name: str, queue: Queue):
    try:
        ticker = Ticker.model_validate(
            get_history_data(
                ticker=ticker_name,
                start_date=config.start_date,
                end_date=config.end_date,
                interval=config.interval,
            )
        )
        queue.put(ticker)
    except requests.RequestException as e:
        logger.error("Проблема с сетью при обработке %s: %s", ticker.symbol, e)
    except Exception as e:
        logger.error("Не получилось обработать %s: %s", ticker.symbol, e)


def csv_writer(*, queue: Queue):
    Path("tickers").mkdir(parents=True, exist_ok=True)
    while True:
        try:
            ticker: Ticker = queue.get()
            if ticker is None:
                break

            with open(f"tickers/{ticker.symbol}.csv", "w", newline="") as file:
                writer = DictWriter(file, fieldnames=ticker.fieldnames())
                writer.writeheader()
                writer.writerows(ticker.to_rows())
                logger.info("%s записан в tickers/%s.csv", ticker.symbol, ticker.symbol)
        except Exception as e:
            logger.error("Ошибка во время записи в csv: %s", e)
        finally:
            queue.task_done()


def parse_tickers_api():
    work_queue = Queue()

    Thread(target=csv_writer, kwargs={'queue': work_queue}, daemon=True).start()

    with ThreadPoolExecutor(max_workers=5) as executor:
        for ticker in get_ticker(config.tickers_file):
            executor.submit(ticker_parser, ticker_name=ticker, queue=work_queue)

    work_queue.join()
    work_queue.put(None)


if __name__ == "__main__":
    parse_tickers_api()
