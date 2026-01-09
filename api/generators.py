import uuid
from datetime import datetime as dt

import numpy as np
from fastapi import HTTPException

DEPTS = ["Engineering", "Sales", "Finance", "HR", "Analyst", "Leadership"]
DEPT_WEIGHTS = [15 / 100, 20 / 100, 20 / 100, 20 / 100, 20 / 100, 5 / 100]
BUILDINGS = ["Sonic", "Re-invent", "Grace", "Maverick"]
MAX_ALLOWED_BATCH = 100_000

def gen_employee_badge_logs(batch_size: int = 100) -> list[dict]:
    if batch_size > MAX_ALLOWED_BATCH:
        raise HTTPException(
            status_code=400,
            detail=f"Limit batch size to {MAX_ALLOWED_BATCH}",
        )

    batch_depts = np.random.choice(DEPTS, size=batch_size, p=DEPT_WEIGHTS)
    batch_buildings = np.random.choice(BUILDINGS, size=batch_size)
    access_granted = np.random.choice([True, False], size=batch_size, p=[50 / 51, 1 / 51])

    start_time = np.datetime64(dt.now())
    random_offsets = np.random.randint(0, 10 * 3600, size=batch_size).astype("timedelta64[s]")
    badge_out_times = start_time + random_offsets

    uuids = [str(uuid.uuid4()) for _ in range(batch_size)]

    return [
        {
            "user_id": uid,
            "dept": str(dept),
            "building_location": str(bldg),
            "badge_in_timestamp": str(start_time),
            "badge_out_timestamp": str(bout),
            "access_granted": bool(acc),
        }
        for uid, dept, bldg, bout, acc
        in zip(uuids, batch_depts, batch_buildings, badge_out_times, access_granted)
    ]

if __name__ == "__main__":
    data = gen_employee_badge_logs()
