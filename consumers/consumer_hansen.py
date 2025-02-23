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
import sys
import time
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
# Fetch lastest data from database
#####################################

def fetch_latest_data(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT grade, subject, test_date, score, student_id FROM test_scores ORDER BY test_date DESC")
    rows = cursor.fetchall()
    data = defaultdict(list)
    for row in rows:
        grade, subject, test_date, score, student_id = row
        data['grade'].append(grade)
        data['subject'].append(subject)
        data['test_date'].append(test_date)
        data['score'].append(score)
        data['student_id'].append(student_id)

    cursor.execute("SELECT grade, subject, AVG(score) FROM test_scores GROUP BY grade, subject")
    averages = cursor.fetchall()
    avg_data = defaultdict(lambda: defaultdict(float))
    for grade, subject, avg_score in averages:
        avg_data[grade][subject] = avg_score

    return data, avg_data

#####################################
# Plot student assessment data
#####################################

def plot_data(data):
    plt.clf()
    subjects = set(data['subject'])
    for subject in subjects:
        x = [datetime.strptime(date, "%Y-%m-%d") for date, sub in zip(data['test_date'], data['subject']) if sub == subject]
        y = [score for score, sub in zip(data['score'], data['subject']) if sub == subject]
        plt.plot(x, y, label=subject)
    plt.legend(loc='upper left')
    plt.xlabel('Test Date')
    plt.ylabel('Score')
    plt.title('Test Scores Over Time')
    plt.grid(True)
    plt.draw()


#####################################
# Plot aggregated assessment data
#####################################
def plot_avg_data(avg_data):
    plt.clf()
    grades = sorted(avg_data.keys())
    for grade in grades:
        subjects = sorted(avg_data[grade].keys())
        avg_scores = [avg_data[grade][subject] for subject in subjects]
        plt.plot(subjects, avg_scores, marker='o', label=f'Grade {grade}')
    plt.legend(loc='upper left')
    plt.xlabel('Subject')
    plt.ylabel('Average Score')
    plt.title('Average Test Scores by Subject and Grade')
    plt.grid(True)
    plt.draw()



#####################################
# Update graph continuously
#####################################

def animate(i, conn, fig, ax1, ax2):
    data, avg_data = fetch_latest_data(conn)
    
    ax1.clear()
    subjects = set(data['subject'])
    for subject in subjects:
        x = [datetime.strptime(date, "%Y-%m-%d") for date, sub in zip(data['test_date'], data['subject']) if sub == subject]
        y = [score for score, sub in zip(data['score'], data['subject']) if sub == subject]
        ax1.plot(x, y, label=subject)
    ax1.legend(loc='upper left')
    ax1.set_xlabel('Test Date')
    ax1.set_ylabel('Score')
    ax1.set_title('Test Scores Over Time')
    ax1.grid(True)
    
    ax2.clear()
    grades = sorted(avg_data.keys())
    for grade in grades:
        subjects = sorted(avg_data[grade].keys())
        avg_scores = [avg_data[grade][subject] for subject in subjects]
        ax2.plot(subjects, avg_scores, marker='o', label=f'Grade {grade}')
    ax2.legend(loc='upper left')
    ax2.set_xlabel('Subject')
    ax2.set_ylabel('Average Score')
    ax2.set_title('Average Test Scores by Subject and Grade')
    ax2.grid(True)

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
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
        ani = animation.FuncAnimation(fig, animate, fargs=(conn, fig, ax1, ax2), interval=5000)

        plt.show()

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