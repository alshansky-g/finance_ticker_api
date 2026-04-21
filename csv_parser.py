from concurrent.futures import InterpreterPoolExecutor
import csv
from pathlib import Path

from config_logging import get_logger

logger = get_logger(__name__)


def get_tickers_filepaths():
    try:
        for ticker_path in Path("tickers").iterdir():
            yield ticker_path
        yield Path("tickers") / "AAPL.csv"

    except FileNotFoundError:
        logger.error(
            "Не найден каталог tickers. Сначала нужно получить информацию по тикерам"
        )


def parse_csv(ticker_file: Path):
    try:
        logger.debug("начинаю парсинг и нормализацию %s", ticker_file)
        with ticker_file.open(encoding="utf-8") as file:
            reader = csv.DictReader(file)
            date, adj_close = [], []
            base = None
            for row in reader:
                if base is None:
                    base = float(row["adj_close"])

                date.append(row["date"])
                adj_close.append(float(row["adj_close"]) * 100 / base)

        logger.debug("парсинг %s успешно закончен", ticker_file)
        return ticker_file.stem, date, adj_close
    except FileNotFoundError:
        logger.error("Файл %s не найден", ticker_file)


def get_tickers_info_for_plot() -> dict[str, tuple[list[str], list[float]]]:
    with InterpreterPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(parse_csv, ticker) for ticker in get_tickers_filepaths()
        ]
        tickers_info = {}
        for f in futures:
            result = f.result()
            if result is None:
                continue
            ticker, dates, prices = result
            tickers_info[ticker] = (dates, prices)

    return tickers_info


if __name__ == "__main__":
    print(get_tickers_info_for_plot())
