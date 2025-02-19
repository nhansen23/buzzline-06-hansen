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

def read_message():
    """
    Process a JSON message from a file.
    """
    logger.info(f"Reading messages from {DATA_FILE}")
    try:
        with open(DATA_FILE, "r") as file:
            data = json.load(file)
            logger.info(f"Read data: {data}")

            if isinstance(data, dict):
                return data  # Return the message if it's a single dictionary
            elif isinstance(data, list) and len(data) > 0:
                return data[-1]  # Return the latest message if it's a list
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logger.error(f"Error reading message: {e}")
        return None
    return None

# Create the data directory if it doesn't exist
    if not os.path.exists('data'):
        os.makedirs('data')

        # Connect to the SQLite3 database in the data directory
        conn = sqlite3.connect('data/messages.db')

        # Insert the message into the database
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO messages (grade, subject, test_date, score, student_id)
            VALUES (?, ?, ?, ?, ?)
        ''', (grade, subject, test_date, score, student_id))
        conn.commit()
# Open a connection to the SQLite3 database
conn = sqlite3.connect('data/messages.db')

# Generate messages and insert into the database
message_generator = generate_messages(conn)

# Generate a few messages (for example, 10 messages)
for _ in range(10):
    print(next(message_generator))

# Close the database connection
conn.close()