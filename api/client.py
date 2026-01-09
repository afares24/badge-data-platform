import logging
import requests
import time
import os
import ujson
from dotenv import load_dotenv
from functools import wraps
from requests.exceptions import RequestException

# init logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# inject env variables
load_dotenv()

# --- utils ---
def retries(max_attempts:int=5):
    def decorator(func):
        @wraps(func)
        def wrap(*args, **kwargs):
            try:
                res = func(*args, **kwargs)
                return res
            except RequestException as re:
                logger.error(f"Request exception was hit: {re}, retrying...")
                # retry logic here
                i,backoff = 0,1
                while i < max_attempts:
                    try:
                        res = func(*args, **kwargs)
                        return res
                    except RequestException:
                        logger.error(f"Retry attempt: {i+1}/{max_attempts}")
                        # dont sleep on last attempt
                        if i < (max_attempts-1): 
                            time.sleep(3**backoff)
                    i+=1
                    backoff+=1
                
                logger.error(f"Retries attempts ({i}) exhausted")
                raise # gets propogated to main exception

            except Exception as e:
                logger.error(f"There was an unexpected error: {e}")
                raise

        return wrap
    return decorator



# --- route paths ---
# employee badge logs
@retries(max_attempts=5)
def get_employee_badge_logs(batch_size: int = 100) -> list[dict]:
    url = os.environ.get("EMPLOYEE_BADGE_LOGS_URL", None)
    if not url:
        raise Exception("Missing url.")

    response = requests.get(
        url, 
        params={"batch": batch_size}
    )
    if response.status_code == 200:
        logger.info("Success. 200 status code")
        return ujson.loads(response.text)
    else:
        logger.error(f"Error. Status code: {response.status_code}")
        detail = ujson.loads(response.text).get("detail", None)
        error_msg = str(detail) or "Bad request"
        raise Exception(f"Error. Status code: {response.status_code}, msg: {error_msg}")

