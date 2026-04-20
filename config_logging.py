from logging import FileHandler, Formatter, StreamHandler, getLogger
import logging
from pathlib import Path
import sys


def get_logger(name: str):
    Path('logs').mkdir(exist_ok=True)

    logger = getLogger(name)
    logger.setLevel(logging.INFO)
    fmt = Formatter('%(asctime)s | %(levelname)-5s | %(message)s', datefmt='%d.%m.%YT%H:%M:%S %z')

    file_handler = FileHandler('logs/errors.log', encoding='utf-8', delay=True)
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(fmt)

    out_handler = StreamHandler(sys.stdout)
    out_handler.setFormatter(fmt)

    logger.addHandler(out_handler)
    logger.addHandler(file_handler)
    return logger