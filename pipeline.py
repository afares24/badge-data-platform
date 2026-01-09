import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable

import duckdb as db
import pandas as pd
from dotenv import load_dotenv

from api.client import get_employee_badge_logs
from utils.tools import logging_factory, sql_monitoring, timing
from compaction_monitoring import start_compaction_watcher

logger = logging_factory(__name__)

load_dotenv()

DATA_LAKE_PATH: Path = Path(os.environ["DATA_LAKE_PATH"])
LANDING_PATH: Path = DATA_LAKE_PATH / "landing"
TEMP_PATH: Path = DATA_LAKE_PATH / "temp"

# build data lake dirs
LANDING_PATH.mkdir(parents=True, exist_ok=True)
TEMP_PATH.mkdir(parents=True, exist_ok=True)

def truncate_lake() -> None:
    """Deletes data in data_lake"""
    for path in [TEMP_PATH, LANDING_PATH]:
        shutil.rmtree(path, ignore_errors = True)
        path.mkdir(parents = True, exist_ok = True)
    logger.info("Truncated data lake.")

@timing
def write_to_parquet(df: pd.DataFrame) -> None:
    now: str = datetime.now().strftime("%Y%m%d%H%M%S")
    temp_file: Path = TEMP_PATH / f"{now}_EBL.parquet"
    landing_file: Path = LANDING_PATH / f"{now}_EBL.parquet"

    df.to_parquet(temp_file, engine="pyarrow", compression="snappy", index=False)
    temp_file.rename(landing_file)
    logger.info(f"Successfully wrote data to {landing_file}")


def _parquet_source() -> str:
    return f"read_parquet('{LANDING_PATH}/*.parquet')"


def read_from_parquet(cols: str = "*") -> db.DuckDBPyRelation:
    """Ad-hoc function for full table scans/ queries"""
    if not os.listdir(LANDING_PATH):
        return

    return db.sql(
        query=f"""
        SELECT {cols}
        FROM {_parquet_source()}
        """
    )


@sql_monitoring
def access_distribution() -> db.DuckDBPyRelation:
    """
    Analyzes badge access grant/deny distribution.

    Returns:
        DuckDBPyRelation with columns:
            - access_granted (bool): Whether access was granted
            - cnts (int): Number of occurrences
    """
    return db.sql(f"""
        SELECT
            access_granted,
            COUNT(*) AS cnts
        FROM {_parquet_source()}
        GROUP BY 1
        ORDER BY COUNT(*) DESC
        """)


@sql_monitoring
def premature_badge_outs() -> db.DuckDBPyRelation:
    """
    Identifies badge sessions under 4 hours by department and location.

    Useful for detecting early departures or potential compliance issues.

    Returns:
        DuckDBPyRelation with columns:
            - dept (str): Department name
            - building_location (str): Building identifier
            - cnts (int): Number of premature badge-outs
    """
    return db.sql(f"""
        WITH premature_badge_outs AS (
        SELECT
            user_id,
            dept,
            building_location,
            DATE_DIFF('hour', badge_in_timestamp::timestamp, badge_out_timestamp::timestamp) AS duration
        FROM {_parquet_source()}
        WHERE duration < 4)

        SELECT
            dept,
            building_location,
            COUNT(*) AS cnts
        FROM premature_badge_outs
        GROUP BY dept, building_location
        ORDER BY COUNT(*) DESC
        """)


@sql_monitoring
def compliance_distribution() -> db.DuckDBPyRelation:
    """
    Analyzes distribution of compliant vs non-compliant badge sessions.

    Compliant: Employee badged in for 4+ hours.
    Non-compliant: Employee badged in for less than 4 hours.

    Returns:
        DuckDBPyRelation with columns:
            - compliance_type (str): 'compliant' or 'non-compliant'
            - cnts (int): Number of sessions
    """
    return db.sql(f"""
    WITH core AS (
        SELECT
            user_id,
            DATE_DIFF('hour', badge_in_timestamp::timestamp, badge_out_timestamp::timestamp) AS duration
        FROM {_parquet_source()}
    )
    SELECT
        CASE WHEN duration >= 4
             THEN 'compliant' ELSE 'non-compliant'
             END AS compliance_type,
        COUNT(*) AS cnts
    FROM core
    GROUP BY 1
    """)


def run(batch_size: int = 100, timeout: int = 300, polling_cadence: int = 10) -> None:
    """
    Orchestrates data ingestion from api to data lake.

    Args:
        batch_size(int):      amount of records in a batch (100_000 limit)
        timeout(int):         data streaming timeout limit (secs)
        polling_cadence(int): duration between data ingests (secs)
    """
    BUFFER_SIZE = 100_000

    end = time.time() + timeout
    buffer = []
    while time.time() < end:
        try:
            batch = get_employee_badge_logs(batch_size)
            buffer.extend(batch)
            if len(buffer) >= BUFFER_SIZE:
                write_to_parquet(pd.DataFrame(buffer))
                logger.info(f"Flushed {len(buffer)} records.")
                # resets buffer
                buffer = []

        except Exception as e:
            logger.error(f"Error during ingestion: {e}")

        time.sleep(polling_cadence)

    # handle remaining data
    if buffer:
        write_to_parquet(pd.DataFrame(buffer))
        logger.info(f"Final flush: {len(buffer)} records.")


if __name__ == "__main__":
    # start fresh
    # truncate_lake()
    observer = start_compaction_watcher()
    try:
        run(batch_size = 100_000, timeout = 120, polling_cadence = 1)
    finally:
        observer.stop()
        observer.join()
