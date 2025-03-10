"""
sqlite_producer.py

Insert records into a sqlite database.

Example record:
"grade": "6",
"subject": "reading",
"test_date": "2024-08-29",
"score": 152,
"student_id": 1584

Environment variables are in utils/utils_config module. 
"""
#####################################
# Import Modules
#####################################

# import from standard library
import random
import json
import pathlib
import sys
import time
import sqlite3
import os
from datetime import datetime, timedelta

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger

def random_weekday(start_date, end_date):
    date_diff = (end_date - start_date).days
    while True:
        random_days = random.randint(0, date_diff)
        test_date = start_date + timedelta(days=random_days)
        if test_date.weekday() < 5:
            return test_date

def generate_messages():
    """
    Generate a stream of JSON messages and insert into SQLite3 database.
    """
    GRADE = ['3', '4', '5', '6', '7', '8']
    SUBJECT = ["Math", "Reading", "Science"]

    # Define the date range
    start_date = datetime.strptime('2023-01-01', '%Y-%m-%d')
    end_date = datetime.strptime('2023-12-31', '%Y-%m-%d')

    while True:
        grade = random.choice(GRADE)
        subject = random.choice(SUBJECT)
        test_date = random_weekday(start_date, end_date).strftime("%Y-%m-%d %H:%M:%S")
        score = random.randint(0, 100)
        student_id = random.randint(1000, 9999)

        # Create JSON message
        json_message = {
            "grade": grade,
            "subject": subject,
            "test_date": test_date,
            "score": score,
            "student_id": student_id,
        }

        yield json_message

def main():
    logger.info("Starting Producer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    try:
        interval_secs = config.get_message_interval_seconds_as_int()
        live_data_path = config.get_live_data_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to delete live data file: {e}")
        sys.exit(2)

    try:
        for message in generate_messages():
            logger.info(message)
            with live_data_path.open("a") as f:
                f.write(json.dumps(message) + "\n")
                logger.info(f"STEP 4a Wrote message to file: {message}")
            time.sleep(interval_secs)
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")

if __name__ == "__main__":
    main()