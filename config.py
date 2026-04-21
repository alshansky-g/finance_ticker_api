from dataclasses import dataclass, field
from datetime import UTC, datetime
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass(slots=True)
class Config:
    tickers_file: str
    base_url: str
    start_date: str
    log_level: str
    interval: str
    end_date: str = field(init=False)

    def __post_init__(self):
        self.end_date = datetime.now(UTC).strftime("%d.%m.%y")


def get_config() -> Config:
    return Config(
        tickers_file=os.environ["TICKERS_FILE"],
        base_url=os.environ["BASE_URL"],
        start_date=os.environ["START_DATE"],
        log_level=os.environ['LOG_LEVEL'],
        interval=os.environ['INTERVAL']
    )


config = get_config()
