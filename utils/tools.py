import logging
import time
from functools import wraps
from typing import Any, Callable

from duckdb import DuckDBPyConnection


def logging_factory(name: str) -> logging.Logger:
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(name)


logger = logging_factory(__name__)


# --- decorators --- #
def sql_monitoring(func: Callable):
    @wraps(func)
    def decorator(*args, **kwargs) -> Any | tuple:
        start = time.perf_counter()
        res = func(*args, **kwargs)
        end = time.perf_counter()
        qs = (str(func.__name__), end - start)
        return res, qs

    return decorator


def timing(func: Callable):
    def decorator(*args, **kwargs):
        start = time.perf_counter()
        res = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"{func.__name__}: {end - start:.3f}s")
        return res

    return decorator
