""" consumer_hansen.py 

Has the following functions:
- init_db(config): Initialize the SQLite database and create the 'streamed_messages' table if it doesn't exist.
- insert_message(message, config): Insert a single processed message into the SQLite database.

{
    "grade": "6",
    "subject": "reading",
    "test_date": "2024-08-29",
    "score": 152,
    "student_id": 1584
}

Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import os
import pathlib
import sqlite3
import json
from collections import defaultdict
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.animation as animation

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger


#####################################
# Define File Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FILE = PROJECT_ROOT.joinpath("data", "score_data.json")
DB_PATH = PROJECT_ROOT.joinpath("score_db.sqlite")

#####################################
# Function to process latest message
#####################################

def create_table(conn):
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS test_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                grade TEXT,
                subject TEXT,
                test_date TEXT,
                score INTEGER,
                student_id INTEGER
            )
        ''')
        conn.commit()
        logger.info("Created test_scores table.")
    except Exception as e:
        logger.error(f"ERROR: Failed to create table: {e}")

def insert_into_db(conn, message):
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO test_scores (grade, subject, test_date, score, student_id)
            VALUES (?, ?, ?, ?, ?)
        ''', (message["grade"], message["subject"], message["test_date"], message["score"], message["student_id"]))
        conn.commit()
        logger.info(f"Inserted message into database: {message}")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into database: {e}")

def main():
    logger.info("Starting Consumer to run continuously.")
    logger.info("Moved .env variables into a utils config module.")

    try:
        live_data_path = config.get_live_data_path()
        sqlite_path = config.get_sqlite_path()
        logger.info(f"SQLITE_PATH: {sqlite_path}")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    try:
        conn = sqlite3.connect(sqlite_path)
        create_table(conn)
        
        while True:
            if live_data_path.exists():
                with live_data_path.open("r") as f:
                    for line in f:
                        message = json.loads(line)
                        insert_into_db(conn, message)
                # Clear the file after reading all messages
                live_data_path.unlink()
            time.sleep(5)
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()