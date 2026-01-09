from fastapi import FastAPI

from api.generators import DataGenerators

# entry point
app = FastAPI()


# --- routes --- #
@app.get("/")
def home():
    return {"message": "Welcome to home page"}


@app.get("/employee-badge-logs/")
def employee_badge_logs(batch: int = 100):
    return DataGenerators().gen_employee_badge_logs(batch_size=batch)


@app.get("/health/")
def health():
    return {"status": "OK"}
