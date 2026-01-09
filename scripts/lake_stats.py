import os
from dotenv import load_dotenv
from pathlib import Path

import humanize

load_dotenv()

DATA_LAKE_PATH: Path = Path(os.environ["DATA_LAKE_PATH"])
LANDING_PATH = DATA_LAKE_PATH / "landing"

def get_data_lake_size() -> None:
    files = list(LANDING_PATH.glob("*.parquet"))
    total_bytes = sum(f.stat().st_size for f in files)

    print(
        f"total size: {humanize.naturalsize(total_bytes)}\n"
        f"total bytes: {total_bytes}\n"
        f"total files: {len(files)}"
    )

if __name__ == "__main__":
    get_data_lake_size()
