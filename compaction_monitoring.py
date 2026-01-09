import time
import os
from pathlib import Path
from datetime import datetime

import duckdb as db
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from utils.tools import logging_factory

logger = logging_factory(__name__)

DATA_LAKE: Path = Path(os.environ["DATA_LAKE_PATH"])
LANDING_PATH: Path = DATA_LAKE / "landing"
TEMP_PATH: Path = DATA_LAKE / "temp"

def compact_landing(landing_path: Path) -> None:
    """Compacts multiple parquet files into single file when threshold exceeded."""
    now = datetime.now().strftime("%Y%m%d%H%M%S%f")

    files_to_compact = [f for f in landing_path.glob("*.parquet")
                        if not f.name.startswith("compacted_")]
    if len(files_to_compact) <= 1:
        return

    temp_file = TEMP_PATH / f"compacted_{now}_EBL.parquet"
    final_file = landing_path / f"compacted_{now}_EBL.parquet"

    try:
        file_list = [str(f) for f in files_to_compact]
        db.sql(f"""
            COPY (SELECT * FROM read_parquet({file_list}))
            TO '{temp_file}' (FORMAT PARQUET, CODEC 'snappy')
        """)

        # atomic to prevent locks
        temp_file.rename(final_file)

        # delete old files
        for f in files_to_compact:
            f.unlink()

        logger.info(f"Compacted {len(files_to_compact)} files in {landing_path.name}")

    except Exception as e:
        # clean up temp on errors
        if temp_file.exists():
            temp_file.unlink()
        logger.error(f"Compaction failed for {landing_path}: {e}")


class DataLakeWatcher(FileSystemEventHandler):
    def on_created(self, event):
        """Triggered when file lands in LANDING_PATH"""
        if event.src_path.endswith(".parquet"):
            landing_path = Path(event.src_path).parent
            self.check_and_compact(landing_path)

    @staticmethod
    def check_and_compact(landing_path: Path) -> None:
        files = [f for f in landing_path.glob("*.parquet")
                     if not f.name.startswith("compacted_")]
        if len(files) > 50: # targeting 200mb based on buffer
            logger.info(f"Compacting {landing_path.name}: {len(files)} files")
            compact_landing(landing_path)


def start_compaction_watcher():
    event_handler = DataLakeWatcher()
    observer = Observer()
    observer.schedule(event_handler, str(LANDING_PATH), recursive=False)
    observer.start()
    logger.info("Compaction watcher started")
    return observer
