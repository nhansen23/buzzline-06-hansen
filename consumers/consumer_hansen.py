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
# DB_PATH = PROJECT_ROOT.joinpath("score_db.sqlite")

#####################################
# Read the JSON data
#####################################

def read_json(json_path):
    if json_path.exists():
        with open(json_path, 'r') as file:
            data = json.load(file)
            avg_data = defaultdict(lambda: defaultdict(float))
            for entry in data:
                grade = entry['grade']
                subject = entry['subject']
                avg_data[grade][subject] += entry['score']
            # Convert sums to averages
            for grade in avg_data:
                for subject in avg_data[grade]:
                    avg_data[grade][subject] /= len([entry for entry in data if entry['grade'] == grade and entry['subject'] == subject])
            return avg_data
    return defaultdict(lambda: defaultdict(float))


#####################################
# Fetch lastest data from database
#####################################

# def fetch_latest_data(conn):
#    cursor = conn.cursor()
#    cursor.execute("SELECT grade, subject, score, student_id FROM test_scores ORDER BY grade DESC")
#    rows = cursor.fetchall()
#    data = defaultdict(list)
#    for row in rows:
#        grade, subject, score, student_id = row
#        data['grade'].append(grade)
#        data['subject'].append(subject)
#        data['score'].append(score)
#        data['student_id'].append(student_id)

#    cursor.execute("SELECT grade, subject, AVG(score) FROM test_scores GROUP BY grade, subject")
#    averages = cursor.fetchall()
#    avg_data = defaultdict(lambda: defaultdict(float))
#    for grade, subject, avg_score in averages:
#        avg_data[grade][subject] = avg_score

#    return data, avg_data

#####################################
# Plot student assessment data
#####################################

# def plot_data(data):
#    plt.clf()
#    subjects = set(data['subject'])
#    student_ids = set(data['student_id'])
#    for subject in subjects:
#        x = [student_id for student_id, sub in zip(data['student_id'], data['subject']) if sub == subject]
#        y = [score for score, sub in zip(data['score'], data['subject']) if sub == subject]
#        plt.plot(x, y, marker='o', label=subject)
#    plt.legend(loc='upper left')
#    plt.xlabel('Student ID')
#    plt.ylabel('Score')
#    plt.title('Student Scores by Subject')
#    plt.grid(True)
#    plt.draw()


#####################################
# Plot aggregated assessment data
#####################################
def plot_avg_data(avg_data):
    plt.clf()
    grades = sorted(avg_data.keys())
    subjects = sorted(set(subject for grade in avg_data for subject in avg_data[grade]))

    for subject in subjects:
        avg_scores = [avg_data[grade][subject] if subject in avg_data[grade] else None for grade in grades]
        plt.plot(grades, avg_scores, marker='o', label=subject)
    
    plt.legend(loc='upper left')
    plt.xlabel('Grade')
    plt.ylabel('Average Score')
    plt.title('Average Test Scores by Grade and Subject')
    plt.grid(True)
    

#####################################
# Update graph continuously
#####################################

def animate(i, json_path, ax):
    avg_data = read_json(json_path)
    
    ax.clear()
    grades = sorted(avg_data.keys())
    subjects = sorted(set(subject for grade in avg_data for subject in avg_data[grade]))
    for subject in subjects:
        avg_scores = [avg_data[grade][subject] if subject in avg_data[grade] else None for grade in grades]
        ax.plot(grades, avg_scores, marker='o', label=subject)
    ax.legend(loc='upper left')
    ax.set_xlabel('Grade')
    ax.set_ylabel('Average Score')
    ax.set_title('Average Test Scores by Grade and Subject')
    ax.grid(True)

#####################################
# Function to process latest message
#####################################

def insert_into_json(json_path, message):
    data = []
    if json_path.exists():
        with open(json_path, 'r') as file:
            data = json.load(file)
    data.append(message)
    with open(json_path, 'w') as file:
        json.dump(data, file, indent=4)
    logger.info(f"Inserted message into JSON file: {message}")


#def create_table(conn):
#    try:
#        cursor = conn.cursor()
#        cursor.execute('''
#            CREATE TABLE IF NOT EXISTS test_scores (
#                id INTEGER PRIMARY KEY AUTOINCREMENT,
#                grade TEXT,
#                subject TEXT,
#                test_date TEXT,
#                score INTEGER,
#                student_id INTEGER
#            )
#        ''')
#        conn.commit()
#        logger.info("Created test_scores table.")
#    except Exception as e:
#        logger.error(f"ERROR: Failed to create table: {e}")
#    pass

#def insert_into_db(conn, message):
#    try:
#        cursor = conn.cursor()
#        cursor.execute('''
#            INSERT INTO test_scores (grade, subject, test_date, score, student_id)
#            VALUES (?, ?, ?, ?, ?)
#        ''', (message["grade"], message["subject"], message["test_date"], message["score"], message["student_id"]))
#        conn.commit()
#        logger.info(f"Inserted message into database: {message}")
#    except Exception as e:
#        logger.error(f"ERROR: Failed to insert message into database: {e}")

def main():
    logger.info("Starting Consumer to run continuously.")

    try:
        json_path = DATA_FILE
        logger.info(f"JSON_PATH: {json_path}")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    try:
        fig, ax = plt.subplots(figsize=(10, 8))
        
        plt.ion()  # Enable interactive mode
        ani = animation.FuncAnimation(fig, animate, fargs=(json_path, ax), interval=5000)
    
        # Save the animation using ffmpeg if available
        ani.save('animation.mp4', writer='ffmpeg')

        plt.show()  # Ensure this is called to display the plot

        while True:
            if json_path.exists():
                with json_path.open("r") as f:
                    for line in f:
                        message = json.loads(line)
                        insert_into_json(json_path, message)
                # Clear the file after reading all messages
                json_path.unlink()
            time.sleep(5)
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")

if __name__ == "__main__":
    