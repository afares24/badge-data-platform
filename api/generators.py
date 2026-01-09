import uuid
from datetime import datetime as dt

import numpy as np
import pandas as pd
from fastapi import HTTPException

DEPTS = ["Engineering", "Sales", "Finance", "HR", "Analyst", "Leadership"]
DEPT_WEIGHTS = [15 / 100, 20 / 100, 20 / 100, 20 / 100, 20 / 100, 5 / 100]
BUILDINGS = ["Sonic", "Re-invent", "Grace", "Maverick"]


class DataGenerators:
    def __init__(self):
        self.MAX_ALLOWED_BATCH = 100_000

    def gen_employee_badge_logs(self, batch_size: int = 100) -> list[dict]:
        if batch_size > self.MAX_ALLOWED_BATCH:
            raise HTTPException(
                status_code=400,
                detail=f"Limit batch size to {self.MAX_ALLOWED_BATCH}",
            )

        batch_depts = np.random.choice(DEPTS, size=batch_size, p=DEPT_WEIGHTS)
        batch_buildings = np.random.choice(BUILDINGS[:4], size=batch_size)

        access_granted = np.random.choice(
            [True, False], size=batch_size, p=[50 / 51, 1 / 51]
        )

        start_time = np.datetime64(dt.now())
        # Generate random offsets to simulate different badge timestamps
        random_offsets = np.random.randint(0, 10 * 3600, size=batch_size).astype(
            "timedelta64[s]"
        )
        badge_out_times = start_time + random_offsets
        # Generates user_ids
        uuids = [uuid.uuid4() for _ in range(batch_size)]
        # Intermediary step before converting to json
        df = pd.DataFrame(
            {
                "user_id": uuids,
                "dept": batch_depts,
                "building_location": batch_buildings,
                "badge_in_timestamp": start_time,
                "badge_out_timestamp": badge_out_times,
                "access_granted": access_granted,
            }
        )

        return df.to_dict("records")


if __name__ == "__main__":
    dg = DataGenerators()
    data = dg.gen_employee_badge_logs()
